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
package io.prestosql.plugin.sheets;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.plugin.sheets.provider.SheetsDataProvider;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SheetsClient
{
    private Set<String> schema;
    private final SheetsConfig sheetsConfig;
    private final SheetsDataProvider sheetsDataProvider;
    private static final Logger log = Logger.get(SheetsClient.class);

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.schema = new HashSet<>();
        this.schema.add("sheets");
        this.sheetsConfig = config;
        this.sheetsDataProvider = SheetsDataProvider.getSheetsDataProvider(config);
    }

    public Set<String> getSchemaNames()
    {
        return schema;
    }

    public Set<String> getTableNames(String schema)
    {
        return sheetsDataProvider.listTables();
    }

    public SheetsTable getTable(String schema, String tableName)
    {
        try {
            List<List<Object>> values = sheetsDataProvider.readAllValues(tableName);
            if (values.size() > 0) {
                List<SheetsColumn> columns = new ArrayList<>();
                // Assuming 1st line is always header
                List<Object> header = values.get(0);
                int count = 0;
                for (Object col : header) {
                    if ("".equals(String.valueOf(col))) {
                        col = "col_" + ++count;
                    }
                    Type colType = VarcharType.VARCHAR;
                    columns.add(new SheetsColumn(String.valueOf(col), colType));
                }
                List<List<Object>> dataValues = values.subList(1, values.size()); // removing header info
                return new SheetsTable(tableName, columns, dataValues);
            }
        }
        catch (Exception e) {
            log.error(e, "Error accessing table info [%s]", tableName);
        }
        return null;
    }
}
