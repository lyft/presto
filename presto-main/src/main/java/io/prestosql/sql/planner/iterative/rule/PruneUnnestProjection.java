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
import com.google.common.collect.ListMultimap;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.Expression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.unnest;

public class PruneUnnestProjection
        extends PruneUnusedNestedColumns
{
    private static final Capture<UnnestNode> UNNEST = newCapture();
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(unnest().capturedAs(UNNEST)
                    .with(source().matching(project().capturedAs(PROJECT)))));

    public PruneUnnestProjection(Metadata metadata)
    {
        super(metadata);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        UnnestNode unnestNode = captures.get(UNNEST);
        ProjectNode projectNode = captures.get(PROJECT);

        ListMultimap<Symbol, Symbol> usedUnnestSymbols = trimNotRowInArray(getUsedUnnestSymbols(parent, unnestNode), context.getSymbolAllocator().getTypes());
        if (usedUnnestSymbols.isEmpty()) {
            return Result.empty();
        }
        // BiMap<Symbol, String> mapBetweenSymbolAndTypeName = getOriginalName(tableScanNode,
        // unnestNode, usedUnnestSymbols, context.getSession(), tableScanNode.getTable());
        // Generate new Symbols
        Map<Symbol, SymbolSet> newSymbols =
                generateNewSymbols(
                        context.getSymbolAllocator().getTypes(),
                        context.getSymbolAllocator(),
                        unnestNode,
                        usedUnnestSymbols,
                        Optional.empty(),
                        Collections.emptyMap());
        // Rewrite assignments in projectNode
        Assignments assignments = projectNode.getAssignments();
        Assignments.Builder newAssignmentBuilder = Assignments.builder();
        for (Map.Entry<Symbol, Expression> entry : assignments.entrySet()) {
            if (newSymbols.containsKey(entry.getKey())) {
                Symbol newSymbol = newSymbols.get(entry.getKey()).getSymbol();
                newAssignmentBuilder.put(
                        newSymbol,
                        entry.getValue());
            }
            else {
                newAssignmentBuilder.put(entry);
            }
        }
        Assignments newAssignments = newAssignmentBuilder.build();

        // Rewrite ProjectNode
        ProjectNode newProjectNode = new ProjectNode(context.getIdAllocator().getNextId(), projectNode.getSource(), newAssignments);
        Map<Symbol, List<Symbol>> symbolMap = mergeUnnestSymbols(unnestNode, newSymbols, Collections.emptyMap());
        // Rewrite UnnestNode
        PlanNode newUnnestNode = new UnnestNode(
                context.getIdAllocator().getNextId(),
                newProjectNode,
                unnestNode.getReplicateSymbols(),
                symbolMap,
                unnestNode.getOrdinalitySymbol());
        PlanNode newPlanNode = parent.replaceChildren(ImmutableList.of(newUnnestNode));
        return Result.ofPlanNode(newPlanNode);
    }
}
