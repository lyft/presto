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

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;

import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceThroughFilter
        implements Rule<ProjectNode>
{
    private final Capture<FilterNode> targetCapture = newCapture();

    public PushDownDereferenceThroughFilter(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    private final TypeAnalyzer typeAnalyzer;

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(filter().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Rule.Context context)
    {
        FilterNode filterNode = captures.get(targetCapture);

        // Pushdown superset of dereference expressions from projections and filtering predicate
        ImmutableList<Expression> expressions = ImmutableList.<Expression>builder()
                .addAll(node.getAssignments().getExpressions())
                .add(filterNode.getPredicate())
                .build();

        Map<DereferenceExpression, Symbol> pushdownDereferences = validDereferences(expressions, context, typeAnalyzer, true);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        PlanNode source = filterNode.getSource();

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                source,
                Assignments.builder()
                    .putIdentities(source.getOutputSymbols())
                    .putAll(HashBiMap.create(pushdownDereferences).inverse())
                    .build());

        PushDownDereferencesUtil.DereferenceReplacer dereferenceReplacer = new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences);

        PlanNode newFilterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                projectNode,
                ExpressionTreeRewriter.rewriteWith(dereferenceReplacer, filterNode.getPredicate()));

        Assignments assignments = node.getAssignments().rewrite(dereferenceReplacer);
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newFilterNode, assignments));
    }
}
