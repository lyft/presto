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
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validPushdownThroughProject;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(a_x := a.x)
 *    TargetNode(a)
 *  </pre>
 * to:
 * <pre>
 *  Project(a_x := symbol)
 *    TargetNode(symbol)
 *      Project(symbol := a.x)
 * </pre>
 *
 * The dereference projections on symbols coming from the sources of TargetNode are pushed down. Projections on symbols
 * being synthesized within the TargetNode remain unaffected.
 */
public class PushDownDereferenceThrough<N extends PlanNode>
        implements Rule<ProjectNode>
{
    private final Capture<N> targetCapture = newCapture();
    private final Pattern<N> targetPattern;

    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThrough(Class<N> aClass, TypeAnalyzer typeAnalyzer)
    {
        targetPattern = Pattern.typeOf(requireNonNull(aClass, "aClass is null"));
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(targetPattern.capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        N child = captures.get(targetCapture);
        Map<DereferenceExpression, Symbol> pushdownDereferences = validPushdownThroughProject(context, node, child, typeAnalyzer);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        PlanNode source = getOnlyElement(child.getSources());

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                source,
                Assignments.builder()
                    .putIdentities(source.getOutputSymbols())
                    .putAll(HashBiMap.create(pushdownDereferences).inverse())
                    .build());

        PlanNode newChildNode = child.replaceChildren(ImmutableList.of(projectNode));

        // Sanity check to ensure propagation of new symbols through the new child
        pushdownDereferences.values().stream()
                .forEach(symbol -> checkState(
                    newChildNode.getOutputSymbols().contains(symbol),
                    "output symbols of the new child don't contain %s",
                    symbol));

        Assignments assignments = node.getAssignments().rewrite(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences));
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newChildNode, assignments));
    }
}
