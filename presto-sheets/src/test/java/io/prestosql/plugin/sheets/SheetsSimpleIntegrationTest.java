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
import io.prestosql.Session;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.plugin.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static io.prestosql.plugin.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class SheetsSimpleIntegrationTest
        extends AbstractTestQueryFramework
{
    protected static final String SHEETS = "sheets";
    private static final QueryRunner queryRunner = createQueryRunner();

    public SheetsSimpleIntegrationTest()
    {
        super(new QueryRunnerSupplier()
        {
            @Override
            public QueryRunner get()
                    throws Exception
            {
                return queryRunner;
            }
        });
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(SHEETS)
                .setSchema(SHEETS)
                .build();
    }

    private static QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner;
        try {
            SheetsPlugin sheetsPlugin = new SheetsPlugin();
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            queryRunner.installPlugin(sheetsPlugin);
            queryRunner.createCatalog(SHEETS, SHEETS, ImmutableMap.of("credentials-path", getTestCredentialsPath(),
                    "metadata-sheet-id", TEST_METADATA_SHEET_ID));
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
        return queryRunner;
    }

    @Test
    public void testListTable()
    {
        assertQuery("show tables", "SELECT * FROM (VALUES 'metadata_table', 'number_text')");
    }

    @Test
    public void testDescTable()
    {
        assertQuery("desc number_text", "SELECT * FROM (VALUES('number','varchar','',''), ('text','varchar','',''))");
        assertQuery("desc metadata_table", "SELECT * FROM (VALUES('table name','varchar','',''), ('sheetid_sheetname','varchar','',''), "
                + "('owner','varchar','',''), ('notes','varchar','',''))");
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM number_text", "SELECT 5");
        assertQuery("SELECT number FROM number_text", "SELECT * FROM (VALUES '1','2','3','4','5')");
        assertQuery("SELECT text FROM number_text", "SELECT * FROM (VALUES 'one','two','three','four','five')");
        assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES ('1','one'), ('2','two'), ('3','three'), ('4','four'), ('5','five'))");
    }
}
