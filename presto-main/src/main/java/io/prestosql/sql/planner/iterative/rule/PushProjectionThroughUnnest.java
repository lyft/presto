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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.NestedColumn;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.ConnectorExpressionTranslator;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.Expression;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static io.prestosql.matching.Pattern.typeOf;
import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;

public class PushProjectionThroughUnnest
        implements Rule<ProjectNode>
{
    private static final Capture<UnnestNode> UNNEST = newCapture();
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(unnest().capturedAs(UNNEST)
                    .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public PushProjectionThroughUnnest(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    /**
     * Extract the nested columns which are being referenced by projectNode
     * Example 1: some pruning happened
     * Input:
     * Project[] => [a, c1, c3]
     *     Unnest[replicate=a, unnest=c:array(row(c1,c2,c3,c4,c5))] => [a, c1, c2, c3, c4, c5]
     *
     * Output:
     * Map {
     *     "c": ["c1", "c3"]
     * }
     *
     * Example 2: No pruning required
     * Input:
     * Project[] => [a, c1, c2]
     *     Unnest[replicate=a, unnest=c:array(row(c1,c2))] => [a, c1, c2]
     *
     * Output:
     * Map {}  (An empty map)
     *
     * @param projectNode the projectNode right above the unnestNode
     * @param unnestNode a Cross Join UnnestNode
     * @return Return the nested columns which are being referenced by, or an empty map if no pruning is necessary.
     */
    private ListMultimap<Symbol, Symbol> getUsedUnnestSymbols(ProjectNode projectNode, UnnestNode unnestNode)
    {
        Optional<Set<Symbol>> usedSymbolsSet = pruneInputs(unnestNode.getOutputSymbols(), projectNode.getAssignments().getExpressions());

        if (!usedSymbolsSet.isPresent()) {
            // No pruning
            return MultimapBuilder.treeKeys().arrayListValues().build();
        }

        List<Symbol> usedSymbols = ImmutableList.copyOf(usedSymbolsSet.get());
        if (usedSymbols.size() == 0) {
            return MultimapBuilder.treeKeys().arrayListValues().build();
        }

        // A UnnestNode may unnest multiple columns at once
        return unnestNode.getUnnestSymbols()
                .entrySet()
                .stream()
                .collect(flatteningToImmutableListMultimap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().filter(usedSymbols::contains)));
    }

    /**
     * Prune the set of available inputs to those required by the given expressions.
     * <p>
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    private static Optional<Set<Symbol>> pruneInputs(Collection<Symbol> availableInputs, Collection<Expression> expressions)
    {
        Set<Symbol> availableInputsSet = ImmutableSet.copyOf(availableInputs);
        Set<Symbol> prunedInputs = Sets.filter(availableInputsSet, SymbolsExtractor.extractUnique(expressions)::contains);

        if (prunedInputs.size() == availableInputsSet.size()) {
            return Optional.empty();
        }

        // ignore _unnest columns.
        // TODO: find better method to handle unnest item which are not referenced.
        if (Sets.difference(availableInputsSet, prunedInputs).stream()
                .allMatch(s -> s.getName().contains("_unnest"))) {
            return Optional.empty();
        }

        return Optional.of(prunedInputs);
    }

    /**
     * Generate new symbols with pruned columns as the type.
     * Example:
     * Input:
     *     unnestNode = Unnest[replicate=a, unnest=c:array(row(c1,c2,c3,c4,c5))]
     *     usedUnnestSymbols = Map {"c":["c1","c3"]}
     *
     * Output:
     * Map{
     *     c: {
     *         c' : array(row(c1,c3)) : [c1, c3]
     *     }
     * }
     * @param tp TypeProvider for this session
     * @param symbolAllocator SymbolAllocator for this session
     * @param unnestNode Original unnestNode
     * @param usedUnnestSymbols The nested columns which are being referenced by projectNode
     * @return The key is the new symbols and the value is the pruned child columns (original child symbols).
     */
    private Map<Symbol, SymbolSet> generateNewSymbols(TypeProvider tp, SymbolAllocator symbolAllocator, UnnestNode unnestNode, ListMultimap<Symbol, Symbol> usedUnnestSymbols, BiMap<Symbol, String> symbolToOriginalName)
    {
        ImmutableMap.Builder<Symbol, SymbolSet> unnestSymbols = ImmutableMap.builder();
        // For each nested column handled by unnestNode
        for (Map.Entry<Symbol, List<Symbol>> unnestColumn : unnestNode.getUnnestSymbols().entrySet()) {
            if (!usedUnnestSymbols.containsKey(unnestColumn.getKey())) {
                continue;
            }
            // Get the name of the used symbols for this nested column
            List<Symbol> usedSymbols = usedUnnestSymbols.get(unnestColumn.getKey());

            // Get the used symbols for this nested column
            ImmutableList<Symbol> remaining = unnestColumn.getValue().stream()
                    .filter(usedSymbols::contains)
                    .collect(ImmutableList.toImmutableList());

            // Create a new symbol with pruned columns as the type
            Symbol newSymbol = createPrunedSymbol(unnestColumn.getKey(), tp.get(unnestColumn.getKey()), usedSymbols, symbolAllocator, symbolToOriginalName);
            SymbolSet symbolSet = new SymbolSet(newSymbol, remaining);
            unnestSymbols.put(unnestColumn.getKey(), symbolSet);
        }
        return unnestSymbols.build();
    }

    /**
     * Generate a new symbol with pruned columns as the type.
     * If the originalType is not array(row(...)), it will return the original symbol.
     * @param oriSymbol The original symbol
     * @param originalType The type of the original symbol (before pruning)
     * @param usedSymbols The symbols which are being referenced
     * @param symbolAllocator SymbolAllocator for this session
     * @return A new symbol with pruned columns as the type
     */
    private Symbol createPrunedSymbol(Symbol oriSymbol, Type originalType, List<Symbol> usedSymbols, SymbolAllocator symbolAllocator, BiMap<Symbol, String> symbolToOriginalName)
    {
        if (originalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) originalType;
            if (arrayType.getElementType() instanceof RowType) {
                RowType rowType = (RowType) arrayType.getElementType();
                List<RowType.Field> fields = rowType.getFields().stream()
                        .filter(field -> usedSymbols.contains(getSymbolName(symbolToOriginalName, field.getName().get())))
                        .collect(Collectors.toList());
                return symbolAllocator.newSymbol(oriSymbol.getName(), new ArrayType(RowType.from(fields)));
            }
        }
        return oriSymbol;
    }

    private Symbol getSymbolName(BiMap<Symbol, String> symbolToOriginalName, String newName)
    {
        BiMap<String, Symbol> inverse = symbolToOriginalName.inverse();
        return inverse.get(newName);
    }

    private List<RowType.Field> extractNestedTypes(Type type)
    {
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            if (arrayType.getElementType() instanceof RowType) {
                return ((RowType) arrayType.getElementType()).getFields();
            }
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Type is not supported. Must be array(row(...)).");
    }

    private BiMap<Symbol, String> getOriginalName(TableScanNode tableScanNode, UnnestNode unnestNode, ListMultimap<Symbol, Symbol> usedUnnestSymbols, Session session, TableHandle tableHandle)
    {
        Map<Symbol, ColumnHandle> assignments = tableScanNode.getAssignments();
        ImmutableBiMap.Builder<Symbol, String> builder = ImmutableBiMap.builder();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : usedUnnestSymbols.asMap().entrySet()) {
            ColumnHandle handle = assignments.get(entry.getKey());
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, handle);
            if (!isRowInArray(columnMetadata.getType())) {
                continue;
            }
            List<RowType.Field> nestedTypes = extractNestedTypes(columnMetadata.getType());
            Map<Symbol, RowType.Field> originalName = getOriginalName(unnestNode, entry.getKey(), nestedTypes);
            originalName.forEach((key, value) -> builder.put(key, value.getName().get()));
            builder.put(entry.getKey(), columnMetadata.getName());
        }

        return builder.build();
    }

    private ListMultimap<Symbol, Symbol> trimNotRowInArray(ListMultimap<Symbol, Symbol> usedUnnestSymbols, TypeProvider tp)
    {
        ListMultimap<Symbol, Symbol> pruned = MultimapBuilder.treeKeys().arrayListValues().build();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : usedUnnestSymbols.asMap().entrySet()) {
            if (isRowInArray(tp.get(entry.getKey()))) {
                pruned.putAll(entry.getKey(), entry.getValue());
            }
        }
        return pruned;
    }

    private boolean isRowInArray(Type type)
    {
        if (type instanceof ArrayType) {
            return ((ArrayType) type).getElementType() instanceof RowType;
        }
        return false;
    }

    private Map<Symbol, RowType.Field> getOriginalName(UnnestNode unnestNode, Symbol column, List<RowType.Field> nestedTypes)
    {
        List<Symbol> symbols = unnestNode.getUnnestSymbols().get(column);
        ImmutableMap.Builder<Symbol, RowType.Field> builder = ImmutableMap.builder();
        for (int i = 0; i < symbols.size(); i++) {
            builder.put(symbols.get(i), nestedTypes.get(i));
        }
        return builder.build();
    }

    private Map<String, List<String>> transform(ListMultimap<Symbol, Symbol> original, BiMap<Symbol, String> mapBetweenSymbolAndTypeName)
    {
        ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : original.asMap().entrySet()) {
            ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
            for (Symbol s : entry.getValue()) {
                listBuilder.add(mapBetweenSymbolAndTypeName.get(s));
            }

            builder.put(mapBetweenSymbolAndTypeName.get(entry.getKey()), listBuilder.build());
        }
        return builder.build();
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        TableScanNode tableScanNode = captures.get(TABLE_SCAN);
        UnnestNode unnestNode = captures.get(UNNEST);

        ListMultimap<Symbol, Symbol> usedUnnestSymbols = trimNotRowInArray(getUsedUnnestSymbols(parent, unnestNode), context.getSymbolAllocator().getTypes());
        if (usedUnnestSymbols.isEmpty()) {
            return Result.empty();
        }

        BiMap<Symbol, String> mapBetweenSymbolAndTypeName = getOriginalName(tableScanNode, unnestNode, usedUnnestSymbols, context.getSession(), tableScanNode.getTable());

        // Get new ColumnHandles using the Metadata API
        Map<String, ColumnHandle> prunedColumnHandles = metadata.getNestedColumnHandles(context.getSession(), tableScanNode.getTable(), transform(usedUnnestSymbols, mapBetweenSymbolAndTypeName));
        if (prunedColumnHandles.isEmpty()) {
            return Result.empty();
        }

        // Generate new Symbols
        Map<Symbol, SymbolSet> newSymbols = generateNewSymbols(context.getSymbolAllocator().getTypes(), context.getSymbolAllocator(), unnestNode, usedUnnestSymbols, mapBetweenSymbolAndTypeName);

        // Rewrite assignments in tableScanNode
        Map<Symbol, ColumnHandle> assignments = tableScanNode.getAssignments();
        ImmutableMap.Builder<Symbol, ColumnHandle> newAssignmentBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            if (newSymbols.containsKey(entry.getKey())) {
                newAssignmentBuilder.put(
                        newSymbols.get(entry.getKey()).getSymbol(),
                        prunedColumnHandles.get(mapBetweenSymbolAndTypeName.get(entry.getKey())));
            }
            else {
                newAssignmentBuilder.put(entry);
            }
        }
        Map<Symbol, ColumnHandle> newAssignments = newAssignmentBuilder.build();

        // Rewrite tableScanNode
        TableScanNode newTableScanNode = new TableScanNode(
                context.getIdAllocator().getNextId(),
                tableScanNode.getTable(),
                new ArrayList<>(newAssignments.keySet()),
                newAssignments,
                tableScanNode.getEnforcedConstraint());
        // Rewrite UnnestNode
        PlanNode newUnnestNode = new UnnestNode(
                context.getIdAllocator().getNextId(),
                newTableScanNode,
                unnestNode.getReplicateSymbols(),
                mergeUnnestSymbols(unnestNode, newSymbols),
                unnestNode.getOrdinalitySymbol());
        return Result.ofPlanNode(parent.replaceChildren(ImmutableList.of(newUnnestNode)));
    }

    private Map<Symbol, List<Symbol>> mergeUnnestSymbols(UnnestNode unnestNode, Map<Symbol, SymbolSet> newSymbols)
    {
        ImmutableMap.Builder<Symbol, List<Symbol>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, List<Symbol>> unnestSymbol : unnestNode.getUnnestSymbols().entrySet()) {
            if (newSymbols.containsKey(unnestSymbol.getKey())) {
                SymbolSet symbolSet = newSymbols.get(unnestSymbol.getKey());
                builder.put(symbolSet.getSymbol(), symbolSet.getSubSymbols());
            }
            else {
                builder.put(unnestSymbol.getKey(), unnestSymbol.getValue());
            }
        }
        return builder.build();
    }

    private static class SymbolSet
    {
        private Symbol symbol;
        private List<Symbol> subSymbols;

        public SymbolSet(Symbol symbol, List<Symbol> subSymbols)
        {
            this.symbol = symbol;
            this.subSymbols = subSymbols;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public List<Symbol> getSubSymbols()
        {
            return subSymbols;
        }
    }
}