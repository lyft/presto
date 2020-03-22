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
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;

import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.createProjectNodeIfRequired;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validDereferences;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(a_x := a.msg.x)
 *    Join(a_y = b_y) => [a]
 *      Project(a_y := a.msg.y, a := a)
 *          Source(a)
 *      Project(b_y := b.msg.y)
 *          Source(b)
 *  </pre>
 * to:
 * <pre>
 *  Project(a_x := symbol)
 *    Join(a_y = b_y) => [symbol]
 *      Project(symbol := a.msg.x, a_y := a.msg.y, a := a)
 *        Source(a)
 *      Project(b_y := b.msg.y)
 *        Source(b)
 * </pre>
 */
public class PushDownDereferenceThroughJoin
        implements Rule<ProjectNode>
{
    private final Capture<JoinNode> targetCapture = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughJoin(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(join().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(targetCapture);

        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        expressionsBuilder.addAll(node.getAssignments().getExpressions());
        joinNode.getFilter().ifPresent(expressionsBuilder::add);
        joinNode.getCriteria().forEach(criteria -> {
            expressionsBuilder.add(criteria.getLeft().toSymbolReference());
            expressionsBuilder.add(criteria.getRight().toSymbolReference());
        });

        Map<DereferenceExpression, Symbol> pushdownDereferences = validDereferences(expressionsBuilder.build(), context, typeAnalyzer, true);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        Assignments.Builder leftSideDereferences = Assignments.builder();
        Assignments.Builder rightSideDereferences = Assignments.builder();

        HashBiMap.create(pushdownDereferences).inverse().entrySet().stream()
                .forEach(entry -> {
                    Symbol baseSymbol = getBase(entry.getValue());
                    if (joinNode.getLeft().getOutputSymbols().contains(baseSymbol)) {
                        leftSideDereferences.put(entry.getKey(), entry.getValue());
                    }
                    else {
                        rightSideDereferences.put(entry.getKey(), entry.getValue());
                    }
                });

        PlanNode leftNode = createProjectNodeIfRequired(joinNode.getLeft(), leftSideDereferences.build(), context.getIdAllocator());
        PlanNode rightNode = createProjectNodeIfRequired(joinNode.getRight(), rightSideDereferences.build(), context.getIdAllocator());

        JoinNode newJoinNode = new JoinNode(context.getIdAllocator().getNextId(),
                joinNode.getType(),
                leftNode,
                rightNode,
                joinNode.getCriteria(),
                ImmutableList.<Symbol>builder()
                    .addAll(joinNode.getOutputSymbols())
                    .addAll(pushdownDereferences.values())
                    .build(),
                joinNode.getFilter().map(expression -> ExpressionTreeRewriter.rewriteWith(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences), expression)),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());

        Assignments assignments = node.getAssignments().rewrite(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences));
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newJoinNode, assignments));
    }
}
