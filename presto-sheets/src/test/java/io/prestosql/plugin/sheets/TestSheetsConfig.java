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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

@Test
public class TestSheetsConfig
{
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(SheetsConfig.class)
                .setCredentialsFilePath(null)
                .setMetadataSheetId(null)
                .setSheetsDataExpireAfterAccessMinutes(1)
                .setSheetsDataExpireAfterWriteMinutes(5));
    }

    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("credentials-path", "/foo/bar/credentials.json")
                .put("metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("sheets-data-expire-after-access-minutes", "2")
                .put("sheets-data-expire-after-write-minutes", "10")
                .build();
        SheetsConfig expected = new SheetsConfig()
                .setCredentialsFilePath("/foo/bar/credentials.json")
                .setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                .setSheetsDataExpireAfterAccessMinutes(2)
                .setSheetsDataExpireAfterWriteMinutes(10);
        assertFullMapping(properties, expected);
    }
}
