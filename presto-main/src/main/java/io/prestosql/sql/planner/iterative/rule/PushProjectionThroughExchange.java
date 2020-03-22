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
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.exchange;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.stream.Collectors.toList;

/**
 * Transforms:
 * <pre>
 *  Project(x = e1, y = e2)
 *    Exchange()
 *      Source(a, b, c)
 *  </pre>
 * to:
 * <pre>
 *  Exchange()
 *    Project(x = e1, y = e2)
 *      Source(a, b, c)
 *  </pre>
 * Or if Exchange needs symbols from Source for partitioning or as hash symbol to:
 * <pre>
 *  Project(x, y)
 *    Exchange()
 *      Project(x = e1, y = e2, a)
 *        Source(a, b, c)
 *  </pre>
 * To avoid looping this optimizer will not be fired if upper Project contains just symbol references.
 */
public class PushProjectionThroughExchange
        implements Rule<ProjectNode>
{
    private static final Capture<ExchangeNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(project -> !isSymbolToSymbolProjection(project))
            .with(source().matching(exchange().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        ExchangeNode exchange = captures.get(CHILD);
        Set<Symbol> partitioningColumns = exchange.getPartitioningScheme().getPartitioning().getColumns();

        // Prepare new partitioning scheme for the exchange node
        List<Symbol> output = new ArrayList<>();
        output.addAll(partitioningColumns);
        // Add hashcolumn if not already added
        exchange.getPartitioningScheme().getHashColumn().ifPresent(hashColumn -> {
            if (!output.contains(hashColumn)) {
                output.add(hashColumn);
            }
        });
        // Add ordering columns if not already added
        exchange.getOrderingScheme().ifPresent(orderingScheme -> {
            for (Symbol orderBySymbol : orderingScheme.getOrderBy()) {
                if (!output.contains(orderBySymbol)) {
                    output.add(orderBySymbol);
                }
            }
        });
        // Add assignments if not already added (this is possible in case of identity non-renaming projections)
        project.getAssignments().getSymbols().forEach(symbol -> {
            if (!output.contains(symbol)) {
                output.add(symbol);
            }
        });

        ImmutableList.Builder<PlanNode> newSourceBuilder = ImmutableList.builder();
        ImmutableList.Builder<List<Symbol>> inputsBuilder = ImmutableList.builder();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            Map<Symbol, SymbolReference> outputToInputMap = extractExchangeOutputToInput(exchange, i);

            Assignments.Builder projections = Assignments.builder();
            List<Symbol> inputs = new ArrayList<>();

            // Need to retain the partition keys for the exchange
            partitioningColumns.stream()
                    .map(outputToInputMap::get)
                    .forEach(nameReference -> {
                        Symbol symbol = Symbol.from(nameReference);
                        projections.put(symbol, nameReference);
                        inputs.add(symbol);
                    });

            if (exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                // Need to retain the hash symbol for the exchange
                exchange.getPartitioningScheme().getHashColumn()
                        .map(outputToInputMap::get)
                        // Do not add the same symbol twice
                        .filter(symbol -> !inputs.contains(symbol))
                        .ifPresent(hashColumnReference -> {
                            Symbol symbol = Symbol.from(hashColumnReference);
                            projections.put(symbol, hashColumnReference);
                            inputs.add(symbol);
                        });
            }

            if (exchange.getOrderingScheme().isPresent()) {
                // need to retain ordering columns for the exchange
                exchange.getOrderingScheme().get().getOrderBy().stream()
                        .map(outputToInputMap::get)
                        // do not project the same symbol twice as ExchangeNode verifies that source input symbols match partitioning scheme outputLayout
                        .filter(symbol -> (!inputs.contains(symbol)))
                        .forEach(nameReference -> {
                            Symbol symbol = Symbol.from(nameReference);
                            projections.put(symbol, nameReference);
                            inputs.add(symbol);
                        });
            }

            for (Map.Entry<Symbol, Expression> projection : project.getAssignments().entrySet()) {
                Expression translatedExpression = inlineSymbols(outputToInputMap, projection.getValue());

                // Skip non-renaming identity projections if already considered, since we do the same for ExchangeNode.
                if (projection.getKey().toSymbolReference().equals(projection.getValue()) &&
                        inputs.stream().map(Symbol::toSymbolReference).collect(toList()).contains(translatedExpression)) {
                    continue;
                }

                Type type = context.getSymbolAllocator().getTypes().get(projection.getKey());
                Symbol symbol = context.getSymbolAllocator().newSymbol(translatedExpression, type);
                projections.put(symbol, translatedExpression);
                inputs.add(symbol);
            }
            newSourceBuilder.add(new ProjectNode(context.getIdAllocator().getNextId(), exchange.getSources().get(i), projections.build()));
            inputsBuilder.add(ImmutableList.copyOf(inputs));
        }

        // output contains all partition and hash symbols so simply swap the output layout
        PartitioningScheme partitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                ImmutableList.copyOf(output),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        PlanNode result = new ExchangeNode(
                exchange.getId(),
                exchange.getType(),
                exchange.getScope(),
                partitioningScheme,
                newSourceBuilder.build(),
                inputsBuilder.build(),
                exchange.getOrderingScheme());

        // we need to strip unnecessary symbols (hash, partitioning columns).
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(project.getOutputSymbols())).orElse(result));
    }

    private static boolean isSymbolToSymbolProjection(ProjectNode project)
    {
        return project.getAssignments().getExpressions().stream().allMatch(e -> e instanceof SymbolReference);
    }

    private static Map<Symbol, SymbolReference> extractExchangeOutputToInput(ExchangeNode exchange, int sourceIndex)
    {
        Map<Symbol, SymbolReference> outputToInputMap = new HashMap<>();
        for (int i = 0; i < exchange.getOutputSymbols().size(); i++) {
            outputToInputMap.put(exchange.getOutputSymbols().get(i), exchange.getInputs().get(sourceIndex).get(i).toSymbolReference());
        }
        return outputToInputMap;
    }
}
