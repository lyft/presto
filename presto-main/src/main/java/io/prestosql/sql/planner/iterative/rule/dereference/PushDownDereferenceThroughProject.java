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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;

import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.dereference.PushDownDereferencesUtil.validPushdownThroughProject;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(msg_x := msg.x)
 *    Project(msg := msg)
 *      Source(msg)
 *  </pre>
 * to:
 * <pre>
 *  Project(msg_x := symbol)
 *    Project(msg := msg, symbol := msg.x)
 *      Source(msg)
 * </pre>
 */
public class PushDownDereferenceThroughProject
        implements Rule<ProjectNode>
{
    private final Capture<ProjectNode> targetCapture = newCapture();

    public PushDownDereferenceThroughProject(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    private final TypeAnalyzer typeAnalyzer;

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(project().capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ProjectNode child = captures.get(targetCapture);
        Map<DereferenceExpression, Symbol> pushdownDereferences = validPushdownThroughProject(context, node, child, typeAnalyzer);

        if (pushdownDereferences.isEmpty()) {
            return Result.empty();
        }

        ProjectNode newChild = new ProjectNode(
                context.getIdAllocator().getNextId(),
                child.getSource(),
                Assignments.builder()
                    .putAll(child.getAssignments())
                    .putAll(HashBiMap.create(pushdownDereferences).inverse())
                    .build());

        Assignments assignments = node.getAssignments().rewrite(new PushDownDereferencesUtil.DereferenceReplacer(pushdownDereferences));
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newChild, assignments));
    }
}
