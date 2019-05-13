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

import io.airlift.json.JsonCodecFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class TestSheetsClient
{
    private SheetsClient sheetsClient;
    private JsonCodecFactory codecFactory;

    @BeforeClass
    public void setUp()
    {
        SheetsConfig sheetsConfig = new SheetsConfig();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
    }

    public void testGetSchemaNames()
    {
    }

    public void getTableNames()
    {
    }

    public void testGetTable()
    {
    }
}
