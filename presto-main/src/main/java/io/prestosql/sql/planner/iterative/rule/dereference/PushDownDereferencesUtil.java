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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.SymbolsExtractor.extractAll;
import static java.util.Objects.requireNonNull;

/**
 * Provides helper methods to push down dereferences in the query plan.
 */
public class PushDownDereferencesUtil
{
    private PushDownDereferencesUtil() {}

    /**
     * Create new symbols for those dereference expressions in the projections of {@param projectNode}, whose base symbol comes
     * from output symbols of the sources of {@param projectNode}'s child.
     */
    static Map<DereferenceExpression, Symbol> validPushdownThroughProject(
            Rule.Context context,
            ProjectNode projectNode,
            PlanNode child,
            TypeAnalyzer typeAnalyzer)
    {
        Map<DereferenceExpression, Symbol> allDereferencesInProject = validDereferences(projectNode.getAssignments().getExpressions(), context, typeAnalyzer, true);

        Set<Symbol> childSourceSymbols = child.getSources().stream()
                .flatMap(node -> node.getOutputSymbols().stream())
                .collect(toImmutableSet());

        return allDereferencesInProject.entrySet().stream()
                .filter(entry -> childSourceSymbols.contains(getBase(entry.getKey())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Create new symbols for dereference expressions extracted from {@param expressions}
     */
    static Map<DereferenceExpression, Symbol> validDereferences(
            Collection<Expression> expressions,
            Rule.Context context,
            TypeAnalyzer typeAnalyzer,
            boolean noOverlap)
    {
        Set<Expression> symbolReferencesAndDereferences = expressions.stream()
                .flatMap(expression -> getSymbolReferencesAndDereferences(expression).stream())
                .collect(Collectors.toSet());

        // Remove overlap if required
        Set<Expression> candidateExpressions = symbolReferencesAndDereferences;
        if (noOverlap) {
            candidateExpressions = symbolReferencesAndDereferences.stream()
                    .filter(expression -> !prefixExists(expression, symbolReferencesAndDereferences))
                    .collect(Collectors.toSet());
        }

        Set<DereferenceExpression> dereferencesToPushdown = candidateExpressions.stream()
                .filter(expression -> (expression instanceof DereferenceExpression))
                .map(expression -> (DereferenceExpression) expression)
                .collect(Collectors.toSet());

        return dereferencesToPushdown.stream()
                .collect(toImmutableMap(Function.identity(), expression -> newSymbol(expression, context, typeAnalyzer)));
    }

    static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<DereferenceExpression, Symbol> expressions;

        DereferenceReplacer(Map<DereferenceExpression, Symbol> expressions)
        {
            this.expressions = requireNonNull(expressions, "expressions is null");
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (expressions.containsKey(node)) {
                return expressions.get(node).toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    static Symbol getBase(DereferenceExpression expression)
    {
        return getOnlyElement(extractAll(expression));
    }

    static PlanNode createProjectNodeIfRequired(PlanNode planNode, Assignments dereferences, PlanNodeIdAllocator idAllocator)
    {
        if (dereferences.isEmpty()) {
            return planNode;
        }
        return new ProjectNode(idAllocator.getNextId(), planNode, Assignments.builder().putIdentities(planNode.getOutputSymbols()).putAll(dereferences).build());
    }

    /**
     * Extract the sub-expressions of type {@link DereferenceExpression} or {@link SymbolReference} from the {@param expression}
     * in a top-down manner. The expressions within the base of a valid {@link DereferenceExpression} sequence are not extracted.
     */
    private static List<Expression> getSymbolReferencesAndDereferences(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
            {
                if (isDereferenceSequence(node)) {
                    context.add(node);
                }
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Expression> context)
            {
                context.add(node);
                return null;
            }

            @Override
            protected Void visitLambdaExpression(LambdaExpression node, ImmutableList.Builder<Expression> context)
            {
                return null;
            }
        }.process(expression, builder);

        return builder.build();
    }

    private static boolean isDereferenceSequence(DereferenceExpression expression)
    {
        return (expression.getBase() instanceof SymbolReference) ||
            ((expression.getBase() instanceof DereferenceExpression) && isDereferenceSequence((DereferenceExpression) (expression.getBase())));
    }

    private static Symbol newSymbol(Expression expression, Rule.Context context, TypeAnalyzer typeAnalyzer)
    {
        Type type = typeAnalyzer.getType(context.getSession(), context.getSymbolAllocator().getTypes(), expression);
        verify(type != null);
        return context.getSymbolAllocator().newSymbol(expression, type);
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;
        while (current instanceof DereferenceExpression) {
            current = ((DereferenceExpression) current).getBase();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof SymbolReference);
        return false;
    }
}
