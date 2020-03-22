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
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.createProjectNodeIfRequired;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.getBase;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validPushdownThroughProject;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceThroughSemiJoin
        implements Rule<ProjectNode>
{
    private final Capture<SemiJoinNode> targetCapture = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughSemiJoin(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(semiJoin().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        SemiJoinNode semiJoinNode = captures.get(targetCapture);
        Map<DereferenceExpression, Symbol> pushdownDereferences = validPushdownThroughProject(context, node, semiJoinNode, typeAnalyzer);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        Assignments.Builder sourceDereferences = Assignments.builder();

        HashBiMap.create(pushdownDereferences).inverse().entrySet().stream()
                .forEach(entry -> {
                    Symbol baseSymbol = getBase(entry.getValue());
                    if (semiJoinNode.getSource().getOutputSymbols().contains(baseSymbol)) {
                        sourceDereferences.put(entry.getKey(), entry.getValue());
                    }
                });

        PlanNode newSource = createProjectNodeIfRequired(semiJoinNode.getSource(), sourceDereferences.build(), context.getIdAllocator());

        // Use the same filteringSource, since output symbols from it are not propagated through SemiJoin
        PlanNode newSemiJoin = semiJoinNode.replaceChildren(ImmutableList.of(newSource, semiJoinNode.getFilteringSource()));

        Assignments assignments = node.getAssignments().rewrite(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences));
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newSemiJoin, assignments));
    }
}
