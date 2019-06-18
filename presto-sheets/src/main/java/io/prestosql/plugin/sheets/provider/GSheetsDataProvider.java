/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.sheets.provider;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.prestosql.plugin.sheets.SheetsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class GSheetsDataProvider
        extends SheetsDataProvider
{
    private static final Logger log = Logger.get(GSheetsDataProvider.class);
    private static final String APPLICATION_NAME = "presto gsheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final List<String> SCOPES =
            Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);

    // tableName <-> sheetId mapping
    private final LoadingCache<String, String> tableSheetMap;

    // sheetId <-> Row List mapping
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private Credential credential;
    private NetHttpTransport httpTransport;
    private Sheets sheetsService;

    public GSheetsDataProvider(SheetsConfig config)
    {
        super(config);
        try {
            this.credential = getCredentials();
            this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            this.sheetsService = new Sheets.Builder(httpTransport, JSON_FACTORY, this.credential).setApplicationName(APPLICATION_NAME).build();
        }
        catch (Exception e) {
            log.error(e, "Unable to load metadata mapping from %s", config.getMetadataSheetId());
        }
        this.tableSheetMap = CacheBuilder.newBuilder()
            .expireAfterAccess(sheetsConfig.getSheetsDataExpireAfterAccessMinutes(), TimeUnit.MINUTES)
            .expireAfterWrite(sheetsConfig.getSheetsDataExpireAfterWriteMinutes(), TimeUnit.MINUTES)
            .build(
                new CacheLoader<String, String>()
                {
                    public String load(String tableName)
                    {
                        try {
                            List<List<Object>> data = sheetDataCache.get(sheetsConfig.getMetadataSheetId());
                            // first line is assumed to be sheet header
                            if (data.size() > 1) {
                                for (int i = 1; i < data.size(); i++) {
                                    if (data.get(i).size() >= 2) {
                                        String tableId = String.valueOf(data.get(i).get(0));
                                        String sheetId = String.valueOf(data.get(i).get(1));
                                        if (tableName.equalsIgnoreCase(tableId)) {
                                            return sheetId;
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error(e, "Error fetching table metadata details");
                        }
                        return null;
                    }
                });

        this.sheetDataCache = CacheBuilder.newBuilder()
            .expireAfterAccess(sheetsConfig.getSheetsDataExpireAfterAccessMinutes(), TimeUnit.MINUTES)
            .expireAfterWrite(sheetsConfig.getSheetsDataExpireAfterWriteMinutes(), TimeUnit.MINUTES)
            .build(
                new CacheLoader<String, List<List<Object>>>()
                {
                    public List<List<Object>> load(String sheetId)
                    {
                        try {
                            return readAllValuesFromSheetExpression(sheetId);
                        }
                        catch (Exception e) {
                            log.error(e, "Error loading table data %", sheetId);
                            throw new IllegalArgumentException(e);
                        }
                    }
                });
    }

    /**
     * @return
     * @throws IOException
     */
    private Credential getCredentials() throws IOException
    {
        GoogleCredential credential = null;
        // Load client secrets.
        try (InputStream in = new FileInputStream(sheetsConfig.getCredentialsFilePath())) {
            credential = GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (Exception e) {
            log.error(e, "Error in loading the credentials from [%s]", sheetsConfig.getCredentialsFilePath());
        }
        return credential;
    }

    public Set<String> listTables()
    {
        Set<String> tables = new HashSet<>();
        try {
            List<List<Object>> tableMetadata = sheetDataCache.get(sheetsConfig.getMetadataSheetId());
            if (tableMetadata.size() > 1) { // we need both table name and sheet id
                for (int i = 1; i < tableMetadata.size(); i++) {
                    if (tableMetadata.get(i).size() > 0) {
                        tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                    }
                }
            }
        }
        catch (Exception e) {
            log.error(e, "Error retrieving table metadata mapping");
        }
        return tables;
    }

    @Override
    public List<List<Object>> readAllValues(String tableName) throws Exception
    {
        String sheetId = tableSheetMap.get(tableName);
        if (null == sheetId) {
            throw new IllegalArgumentException("Table not found " + tableName);
        }
        return sheetDataCache.get(sheetId);
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression) throws Exception
    {
        // by default loading up to max 10k columns
        String defaultRange = "$1:$10000";
        String[] tableOptions = sheetExpression.split("#");
        String sheetId = tableOptions[0];
        if (tableOptions.length > 1) {
            defaultRange = tableOptions[1];
        }
        Sheets.Spreadsheets.Values valuesAll = sheetsService.spreadsheets().values();
        log.info("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
        ValueRange response = valuesAll.get(sheetId, defaultRange).execute();
        List<List<Object>> values = response.getValues();
        return values;
    }
}
