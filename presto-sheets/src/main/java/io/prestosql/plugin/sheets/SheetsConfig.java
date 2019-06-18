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

import io.airlift.configuration.Config;

public class SheetsConfig
{
    private String credentialsFilePath;

    private String metadataSheetId;

    private int sheetsDataExpireAfterAccessMinutes = 1;
    private int sheetsDataExpireAfterWriteMinutes = 5;

    public String getCredentialsFilePath()
    {
        return credentialsFilePath;
    }

    @Config("credentials-path")
    public SheetsConfig setCredentialsFilePath(String credentialsFilePath)
    {
        this.credentialsFilePath = credentialsFilePath;
        return this;
    }

    public String getMetadataSheetId()
    {
        return metadataSheetId;
    }

    @Config("metadata-sheet-id")
    public SheetsConfig setMetadataSheetId(String metadataSheetId)
    {
        this.metadataSheetId = metadataSheetId;
        return this;
    }

    public int getSheetsDataExpireAfterAccessMinutes()
    {
        return sheetsDataExpireAfterAccessMinutes;
    }

    @Config("sheets-data-expire-after-access-minutes")
    public SheetsConfig setSheetsDataExpireAfterAccessMinutes(int sheetsDataExpireAfterAccessMinutes)
    {
        this.sheetsDataExpireAfterAccessMinutes = sheetsDataExpireAfterAccessMinutes;
        return this;
    }

    public int getSheetsDataExpireAfterWriteMinutes()
    {
        return sheetsDataExpireAfterWriteMinutes;
    }

    @Config("sheets-data-expire-after-write-minutes")
    public SheetsConfig setSheetsDataExpireAfterWriteMinutes(int sheetsDataExpireAfterWriteMinutes)
    {
        this.sheetsDataExpireAfterWriteMinutes = sheetsDataExpireAfterWriteMinutes;
        return this;
    }
}
