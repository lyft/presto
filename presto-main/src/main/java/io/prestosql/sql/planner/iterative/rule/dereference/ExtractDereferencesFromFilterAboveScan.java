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
package io.prestosql.sql.planner.iterative.rule.dereference;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Extracts all dereference expressions from a filter node located above a table scan. This enables pushdown of
 * dereference projections from the filter node into table scan using the {@link io.prestosql.sql.planner.iterative.rule.PushProjectionIntoTableScan} rule.
 */
public class ExtractDereferencesFromFilterAboveScan
        implements Rule<FilterNode>
{
    private final TypeAnalyzer typeAnalyzer;
    private final Capture<TableScanNode> targetCapture = newCapture();

    public ExtractDereferencesFromFilterAboveScan(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return filter()
                .with(source().matching(tableScan().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        BiMap<DereferenceExpression, Symbol> expressions =
                HashBiMap.create(validDereferences(ExpressionExtractor.extractExpressionsNonRecursive(node), context, typeAnalyzer, false));

        if (expressions.isEmpty()) {
            return Result.empty();
        }

        PlanNode source = node.getSource();
        Assignments assignments = Assignments.builder()
                .putIdentities(source.getOutputSymbols())
                .putAll(expressions.inverse())
                .build();
        ProjectNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), source, assignments);

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                projectNode,
                ExpressionTreeRewriter.rewriteWith(new PushDownDereferencesUtil.DereferenceReplacer(expressions), node.getPredicate()));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.builder().putIdentities(node.getOutputSymbols()).build()));
    }
}
