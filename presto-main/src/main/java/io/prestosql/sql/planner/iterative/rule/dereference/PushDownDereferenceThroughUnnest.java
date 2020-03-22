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
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validPushdownThroughProject;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceThroughUnnest
        implements Rule<ProjectNode>
{
    private final Capture<UnnestNode> targetCapture = newCapture();

    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughUnnest(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(unnest().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        UnnestNode unnestNode = captures.get(targetCapture);
        Map<DereferenceExpression, Symbol> pushdownDereferences = validPushdownThroughProject(context, node, unnestNode, typeAnalyzer);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        // Create a new ProjectNode (above the original source) containing all dereference projections on replicated symbols
        Assignments assignments = Assignments.builder()
                .putIdentities(unnestNode.getSource().getOutputSymbols())
                .putAll(HashBiMap.create(pushdownDereferences).inverse())
                .build();
        ProjectNode source = new ProjectNode(context.getIdAllocator().getNextId(), unnestNode.getSource(), assignments);

        // Create a new UnnestNode
        UnnestNode newUnnest = new UnnestNode(context.getIdAllocator().getNextId(),
                source,
                ImmutableList.<Symbol>builder()
                    .addAll(unnestNode.getReplicateSymbols())
                    .addAll(pushdownDereferences.values())
                    .build(),
                unnestNode.getUnnestSymbols(),
                unnestNode.getOrdinalitySymbol(),
                unnestNode.getJoinType(),
                unnestNode.getFilter());

        return Result.ofPlanNode(
                new ProjectNode(context.getIdAllocator().getNextId(),
                newUnnest,
                node.getAssignments().rewrite(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences))));
    }
}
