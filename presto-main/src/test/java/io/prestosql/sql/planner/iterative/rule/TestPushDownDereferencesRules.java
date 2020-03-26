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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.dereference.ExtractDereferencesFromFilterAboveScan;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThrough;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThroughFilter;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThroughJoin;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThroughProject;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThroughSemiJoin;
import io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferenceThroughUnnest;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.iterative.rule.test.RuleTester.CATALOG_ID;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestPushDownDereferencesRules
        extends BaseRuleTest
{
    private static final RowType MSG_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

    @Test
    public void testDoesNotFire()
    {
        // rule does not fire for symbols
        tester().assertThat(new PushDownDereferenceThroughFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("x > BIGINT '5'"),
                                p.values(p.symbol("x"))))
                .doesNotFire();

        // pushdown is not enabled if dereferences come from an expression that is not a simple dereference chain
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1"), expression("cast(row(a, b) as row(f1 row(x bigint, y bigint), f2 bigint)).f1"),
                                        p.symbol("expr_2"), expression("cast(row(a, b) as row(f1 row(x bigint, y bigint), f2 bigint)).f1.y")),
                                p.project(
                                        Assignments.of(
                                                p.symbol("a", MSG_TYPE), expression("a"),
                                                p.symbol("b"), expression("b")),
                                        p.values(p.symbol("a", MSG_TYPE), p.symbol("b")))))
                .doesNotFire();

        // Does not fire when base symbols are referenced along with the dereferences
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("expr", MSG_TYPE), expression("a"), p.symbol("a_x"), expression("a.x")),
                                p.project(
                                        Assignments.of(p.symbol("a", MSG_TYPE), expression("a")),
                                        p.values(p.symbol("a", MSG_TYPE)))))
                .doesNotFire();
    }

    @Test
    public void testPushdownDereferenceThroughProject()
    {
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                        p.project(
                                Assignments.of(
                                        p.symbol("y"), expression("y"),
                                        p.symbol("msg", MSG_TYPE), expression("msg")),
                                p.values(p.symbol("msg", MSG_TYPE), p.symbol("y")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                                strictProject(
                                        ImmutableMap.of(
                                                "msg_x", PlanMatchPattern.expression("msg.x"),
                                                "y", PlanMatchPattern.expression("y"),
                                                "msg", PlanMatchPattern.expression("msg")),
                                        values("msg", "y"))));
    }

    @Test
    public void testPushDownDereferenceThroughJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), expression("msg1.x"))
                                        .put(p.symbol("right_y"), expression("msg2.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", MSG_TYPE), p.symbol("unreferenced_symbol")),
                                        p.values(p.symbol("msg2", MSG_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", PlanMatchPattern.expression("x"))
                                        .put("right_y", PlanMatchPattern.expression("y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                join(INNER, ImmutableList.of(),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "x", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1"),
                                                        "unreferenced_symbol", PlanMatchPattern.expression("unreferenced_symbol")),
                                                values("msg1", "unreferenced_symbol")),
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("y", PlanMatchPattern.expression("msg2.y"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .put("msg2", PlanMatchPattern.expression("msg2"))
                                                        .build(),
                                                values("msg2", "z")))));

        // Verify pushdown for filters
        tester().assertThat(new PushDownDereferenceThroughJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr"), expression("msg1.x"),
                                        p.symbol("expr_2"), expression("msg2")),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", MSG_TYPE)),
                                        p.values(p.symbol("msg2", MSG_TYPE)),
                                        p.expression("msg1.x + msg2.y > BIGINT '10'"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "expr", PlanMatchPattern.expression("msg1_x"),
                                        "expr_2", PlanMatchPattern.expression("msg2")),
                                join(INNER, ImmutableList.of(), Optional.of("msg1_x + msg2.y > BIGINT '10'"),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg1_x", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1")),
                                                values("msg1")),
                                        values("msg2"))));
    }

    @Test
    public void testPushdownDereferencesThroughSemiJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughSemiJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), expression("msg1.x"))
                                        .build(),
                                p.semiJoin(
                                        p.symbol("left"),
                                        p.symbol("right"),
                                        p.symbol("match"),
                                        Optional.empty(),
                                        Optional.empty(),
                                        p.values(p.symbol("msg1", MSG_TYPE), p.symbol("left")),
                                        p.values(p.symbol("msg2", MSG_TYPE), p.symbol("right")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", PlanMatchPattern.expression("msg1_x"))
                                        .build(),
                                semiJoin(
                                        "left_symbol",
                                        "right_symbol",
                                        "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg1_x", PlanMatchPattern.expression("msg1.x"),
                                                        "msg1", PlanMatchPattern.expression("msg1"),
                                                        "left", PlanMatchPattern.expression("left_symbol")),
                                                values("msg1", "left_symbol")),
                                        values("msg2", "right_symbol"))));
    }

    @Test
    public void testPushdownDereferencesThroughUnnest()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        tester().assertThat(new PushDownDereferenceThroughUnnest(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                                p.unnest(
                                        ImmutableList.of(p.symbol("msg", MSG_TYPE)),
                                        ImmutableMap.of(p.symbol("arr", arrayType), ImmutableList.of(p.symbol("field"))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(p.symbol("msg", MSG_TYPE), p.symbol("arr", arrayType)))))
                .matches(
                    strictProject(
                            ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                            unnest(
                                    strictProject(
                                            ImmutableMap.of(
                                                    "msg_x", PlanMatchPattern.expression("msg.x"),
                                                    "msg", PlanMatchPattern.expression("msg"),
                                                    "arr", PlanMatchPattern.expression("arr")),
                                            values("msg", "arr")))));

        // Test with dereferences on unnested column
        RowType rowType = rowType(field("f1", BIGINT), field("f2", BIGINT));
        ArrayType nestedColumnType = new ArrayType(rowType(field("f1", BIGINT), field("f2", rowType)));

        tester().assertThat(new PushDownDereferenceThroughUnnest(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("deref_replicate", BIGINT), expression("replicate.f2"),
                                        p.symbol("deref_unnest", BIGINT), expression("unnested_row.f2")),
                                p.unnest(
                                        ImmutableList.of(p.symbol("replicate", rowType)),
                                        ImmutableMap.of(
                                                p.symbol("nested", nestedColumnType),
                                                ImmutableList.of(
                                                        p.symbol("unnested_bigint", BIGINT),
                                                        p.symbol("unnested_row", rowType))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(p.symbol("replicate", rowType), p.symbol("nested", nestedColumnType)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "deref_replicate", PlanMatchPattern.expression("symbol"),
                                        "deref_unnest", PlanMatchPattern.expression("unnested_row.f2")),
                                unnest(
                                        ImmutableList.of("replicate", "symbol"),
                                        ImmutableMap.of("nested", ImmutableList.of("unnested_bigint", "unnested_row")),
                                        Optional.empty(),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "expr", PlanMatchPattern.expression("replicate.f2"),
                                                        "replicate", PlanMatchPattern.expression("replicate"),
                                                        "nested", PlanMatchPattern.expression("nested")),
                                                values("replicate", "nested")))));
    }

    @Test
    public void testExtractDereferencesFromFilterAboveScan()
    {
        TableHandle testTable = new TableHandle(
                new CatalogName(CATALOG_ID),
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());

        RowType nestedMsgType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("nested"), MSG_TYPE)));
        tester().assertThat(new ExtractDereferencesFromFilterAboveScan(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("a.nested.x != 5 AND b.y = 2 AND CAST(a.nested as JSON) is not null"),
                                p.tableScan(
                                        testTable,
                                        ImmutableList.of(p.symbol("a", nestedMsgType), p.symbol("b", MSG_TYPE)),
                                        ImmutableMap.of(
                                                p.symbol("a", nestedMsgType), new TpchColumnHandle("a", nestedMsgType),
                                                p.symbol("b", MSG_TYPE), new TpchColumnHandle("b", MSG_TYPE)))))
                .matches(project(
                        filter("expr != 5 AND expr_0 = 2 AND CAST(expr_1 as JSON) is not null",
                                strictProject(
                                        ImmutableMap.of(
                                                "expr", PlanMatchPattern.expression("a.nested.x"),
                                                "expr_0", PlanMatchPattern.expression("b.y"),
                                                "expr_1", PlanMatchPattern.expression("a.nested"),
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b")),
                                        tableScan(
                                                equalTo(testTable.getConnectorHandle()),
                                                TupleDomain.all(),
                                                ImmutableMap.of(
                                                        "a", equalTo(new TpchColumnHandle("a", nestedMsgType)),
                                                        "b", equalTo(new TpchColumnHandle("b", MSG_TYPE))))))));
    }

    @Test
    public void testPushdownDereferenceThroughFilter()
    {
        tester().assertThat(new PushDownDereferenceThroughFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("expr", BIGINT), expression("msg.x")),
                                p.filter(
                                        expression("msg.x <> 'foo'"),
                                        p.values(p.symbol("msg", MSG_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", PlanMatchPattern.expression("msg_x")),
                                filter(
                                        "msg_x <> 'foo'",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "msg_x", PlanMatchPattern.expression("msg.x"),
                                                        "msg", PlanMatchPattern.expression("msg")),
                                                values("msg")))));
    }

    @Test
    public void testPushDownDereferenceThrough()
    {
        tester().assertThat(new PushDownDereferenceThrough<>(LimitNode.class, tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), expression("msg.x"))
                                        .put(p.symbol("msg_y"), expression("msg.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.limit(10,
                                        p.values(p.symbol("msg", MSG_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg_x", PlanMatchPattern.expression("x"))
                                        .put("msg_y", PlanMatchPattern.expression("y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                limit(10,
                                        strictProject(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", PlanMatchPattern.expression("msg.x"))
                                                        .put("y", PlanMatchPattern.expression("msg.y"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .put("msg", PlanMatchPattern.expression("msg"))
                                                        .build(),
                                                values("msg", "z")))));
    }

    @Test
    public void testMultiLevelPushdown()
    {
        Type complexType = rowType(field("f1", rowType(field("f1", BIGINT), field("f2", BIGINT))), field("f2", BIGINT));
        tester().assertThat(new PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                            Assignments.of(
                                    p.symbol("expr_1"), expression("a.f1"),
                                    p.symbol("expr_2"), expression("a.f1.f1 + 2 + b.f1.f1 + b.f1.f2")),
                            p.project(
                                    Assignments.identity(ImmutableList.of(p.symbol("a", complexType), p.symbol("b", complexType))),
                                    p.values(p.symbol("a", complexType), p.symbol("b", complexType)))))
                .matches(
                strictProject(
                        ImmutableMap.of(
                                "expr_1", PlanMatchPattern.expression("a_f1"),
                                "expr_2", PlanMatchPattern.expression("a_f1.f1 + 2 + b_f1_f1 + b_f1_f2")),
                        strictProject(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "b", PlanMatchPattern.expression("b"),
                                        "a_f1", PlanMatchPattern.expression("a.f1"),
                                        "b_f1_f1", PlanMatchPattern.expression("b.f1.f1"),
                                        "b_f1_f2", PlanMatchPattern.expression("b.f1.f2")),
                                values("a", "b"))));
    }
}
