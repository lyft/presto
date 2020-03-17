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
package io.prestosql.plugin.accumulo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Accumulo requires a unique identifier for the rows.
 * Any row that has a duplicate row ID is effectively an update,
 * overwriting existing values of the row with whatever the new values are.
 * For the lineitem and partsupp tables, there is no unique identifier,
 * so a generated UUID is used in order to prevent overwriting rows of data.
 * This is the same for any test cases that were creating tables with duplicate rows,
 * so some test cases are overridden from the base class and slightly modified to add an additional UUID column.
 */
public class TestAccumuloDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createAccumuloQueryRunner(ImmutableMap.of());
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Accumulo connector does not support column default values");
    }

    @Override
    public void testCreateSchema()
    {
        // schema creation is not supported
    }

    @Override
    public void testAddColumn()
    {
        // Adding columns via SQL are not supported until adding columns with comments are supported
    }

    @Override
    public void testDropColumn()
    {
        // Dropping columns are not supported by the connector
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // This test is overridden due to Function "UUID" not found errors
        // Some test cases from the base class are removed

        // TODO some test cases from overridden method succeed to create table, but with wrong number or rows.

        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (a bigint, b double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_as_if_not_exists AS SELECT cast(uuid() AS uuid) AS uuid, orderkey, discount FROM lineitem", 0);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("DROP TABLE test_create_table_as_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));

        this.assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        this.assertCreateTableAsSelect(
                "test_with_data",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        this.assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");
    }

    @Override
    public void testDelete()
    {
        // Deletes are not supported by the connector
    }

    @Override
    public void testInsert()
    {
        @Language("SQL") String query = "SELECT cast(uuid() AS varchar) AS uuid, orderdate, orderkey FROM orders";

        assertUpdate("CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT orderdate, orderkey FROM test_insert", "SELECT orderdate, orderkey FROM orders");
        // Override because base class error: Cannot insert null row ID
        assertUpdate("INSERT INTO test_insert (uuid, orderkey) VALUES ('000000', -1)", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderdate) VALUES ('000001', DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderkey, orderdate) VALUES ('000002', -2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderdate, orderkey) VALUES ('000003', DATE '2001-01-03', -3)", 1);

        assertQuery("SELECT orderdate, orderkey FROM test_insert",
                "SELECT orderdate, orderkey FROM orders"
                        + " UNION ALL SELECT null, -1"
                        + " UNION ALL SELECT DATE '2001-01-01', null"
                        + " UNION ALL SELECT DATE '2001-01-02', -2"
                        + " UNION ALL SELECT DATE '2001-01-03', -3");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (uuid, orderkey, orderdate) " +
                        "SELECT cast(uuid() AS varchar) AS uuid, orderkey, orderdate FROM orders " +
                        "UNION ALL " +
                        "SELECT cast(uuid() AS varchar) AS uuid, orderkey, orderdate FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");
    }

    @Override
    public void testInsertWithCoercion()
    {
        // Override because of non-canonical varchar mapping
    }

    @Override // Overridden because we currently do not support arrays with null elements
    public void testInsertArray()
    {
        assertUpdate("CREATE TABLE test_insert_array (a ARRAY<DOUBLE>, b ARRAY<BIGINT>)");

        // assertUpdate("INSERT INTO test_insert_array (a) VALUES (ARRAY[null])", 1); TODO support ARRAY with null elements

        assertUpdate("INSERT INTO test_insert_array (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
        assertQuery("SELECT a[1], b[1] FROM test_insert_array", "VALUES (12.3, 12)");

        assertUpdate("DROP TABLE test_insert_array");
    }

    @Test
    public void testInsertDuplicateRows()
    {
        // This test case tests the Accumulo connectors override capabilities
        // When a row is inserted into a table where a row with the same row ID already exists,
        // the cells of the existing row are overwritten with the new values
        try {
            assertUpdate("CREATE TABLE test_insert_duplicate AS SELECT 1 a, 2 b, '3' c", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 2, '3'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, c) VALUES (1, '4')", 1);
            assertUpdate("INSERT INTO test_insert_duplicate (a, b) VALUES (1, 3)", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 3, '4'");
        }
        finally {
            assertUpdate("DROP TABLE test_insert_duplicate");
        }
    }

    @Override
    public void testScalarSubquery()
    {
        // Override because of extra UUID column in lineitem table, cannot SELECT *

        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE orderkey = \n"
                + "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE orderkey = \n"
                + "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE \n"
                + "(SELECT orderkey FROM orders WHERE 0=1) "
                + "is null");
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE \n"
                + "(SELECT orderkey FROM orders WHERE 0=1) "
                + "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        for (String joinType : ImmutableList.of("INNER", "LEFT OUTER")) {
            assertQuery("SELECT l.orderkey, COUNT(*) " +
                    "FROM lineitem l " + joinType + " JOIN orders o ON l.orderkey = o.orderkey " +
                    "WHERE l.orderkey BETWEEN" +
                    "   (SELECT avg(orderkey) FROM orders) - 10 " +
                    "   AND" +
                    "   (SELECT avg(orderkey) FROM orders) + 10 " +
                    "GROUP BY l.orderkey");
        }

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT "
                        + "orderkey, partkey, suppkey, linenumber, quantity, "
                        + "extendedprice, discount, tax, returnflag, linestatus, "
                        + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                        + "FROM lineitem WHERE orderkey = (\n"
                        + "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");
    }

    @Override
    public void testShowColumns()
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        assertEquals(actual.getMaterializedRows().get(0).getField(0), "orderkey");
        assertEquals(actual.getMaterializedRows().get(0).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(1).getField(0), "custkey");
        assertEquals(actual.getMaterializedRows().get(1).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(2).getField(0), "orderstatus");
        assertEquals(actual.getMaterializedRows().get(2).getField(1), "varchar(1)");
        assertEquals(actual.getMaterializedRows().get(3).getField(0), "totalprice");
        assertEquals(actual.getMaterializedRows().get(3).getField(1), "double");
        assertEquals(actual.getMaterializedRows().get(4).getField(0), "orderdate");
        assertEquals(actual.getMaterializedRows().get(4).getField(1), "date");
        assertEquals(actual.getMaterializedRows().get(5).getField(0), "orderpriority");
        assertEquals(actual.getMaterializedRows().get(5).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(6).getField(0), "clerk");
        assertEquals(actual.getMaterializedRows().get(6).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(7).getField(0), "shippriority");
        assertEquals(actual.getMaterializedRows().get(7).getField(1), "integer");
        assertEquals(actual.getMaterializedRows().get(8).getField(0), "comment");
        assertEquals(actual.getMaterializedRows().get(8).getField(1), "varchar(79)");
    }

    @Test
    public void testMultiInBelowCardinality()
    {
        assertQuery("SELECT COUNT(*) FROM partsupp WHERE partkey = 1", "SELECT 4");
        assertQuery("SELECT COUNT(*) FROM partsupp WHERE partkey = 2", "SELECT 4");
        assertQuery("SELECT COUNT(*) FROM partsupp WHERE partkey IN (1, 2)", "SELECT 8");
    }

    @Test
    public void testSelectNullValue()
    {
        try {
            assertUpdate("CREATE TABLE test_select_null_value AS SELECT 1 a, 2 b, CAST(NULL AS BIGINT) c", 1);
            assertQuery("SELECT * FROM test_select_null_value", "SELECT 1, 2, NULL");
            assertQuery("SELECT a, c FROM test_select_null_value", "SELECT 1, NULL");
        }
        finally {
            assertUpdate("DROP TABLE test_select_null_value");
        }
    }

    @Test
    public void testCreateTableEmptyColumns()
    {
        try {
            assertUpdate("CREATE TABLE test_create_table_empty_columns WITH (column_mapping = 'a:a:a,b::b,c:c:,d::', index_columns='a,b,c,d') AS SELECT 1 id, 2 a, 3 b, 4 c, 5 d", 1);
            assertQuery("SELECT * FROM test_create_table_empty_columns", "SELECT 1, 2, 3, 4, 5");
            assertQuery("SELECT * FROM test_create_table_empty_columns WHERE a = 2", "SELECT 1, 2, 3, 4, 5");
            assertQuery("SELECT * FROM test_create_table_empty_columns WHERE b = 3", "SELECT 1, 2, 3, 4, 5");
            assertQuery("SELECT * FROM test_create_table_empty_columns WHERE c = 4", "SELECT 1, 2, 3, 4, 5");
            assertQuery("SELECT * FROM test_create_table_empty_columns WHERE d = 5", "SELECT 1, 2, 3, 4, 5");
        }
        finally {
            assertUpdate("DROP TABLE test_create_table_empty_columns");
        }
    }

    @Override
    public void testDescribeOutput()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testCommentTable()
    {
        // Accumulo connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.startsWith("decimal(")
                || typeName.equals("timestamp with time zone")
                || typeName.startsWith("char(")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }
}
