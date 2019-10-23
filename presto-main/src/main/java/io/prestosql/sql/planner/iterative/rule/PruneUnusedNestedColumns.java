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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.Expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;

public abstract class PruneUnusedNestedColumns
        implements Rule<ProjectNode>
{
    protected final Metadata metadata;

    public PruneUnusedNestedColumns(Metadata metadata)
    {
        this.metadata = metadata;
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
    protected ListMultimap<Symbol, Symbol> getUsedUnnestSymbols(ProjectNode projectNode, UnnestNode unnestNode)
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
    protected Map<Symbol, SymbolSet> generateNewSymbols(TypeProvider tp, SymbolAllocator symbolAllocator, UnnestNode unnestNode, ListMultimap<Symbol, Symbol> usedUnnestSymbols, Optional<BiMap<Symbol, String>> symbolToOriginalName, Map<String, Type> projectionTypes)
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
            Symbol newSymbol = createPrunedSymbol(unnestColumn.getKey(), tp.get(unnestColumn.getKey()), usedSymbols, symbolAllocator, symbolToOriginalName, projectionTypes);
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
    private Symbol createPrunedSymbol(Symbol oriSymbol, Type originalType, List<Symbol> usedSymbols, SymbolAllocator symbolAllocator, Optional<BiMap<Symbol, String>> symbolToOriginalName, Map<String, Type> projectionTypes)
    {
        if (originalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) originalType;
            if (arrayType.getElementType() instanceof RowType) {
                RowType rowType = (RowType) arrayType.getElementType();
                List<RowType.Field> fields = rowType.getFields().stream()
                        .filter(field -> usedSymbols.contains(getSymbolName(symbolToOriginalName, field.getName().get())))
                        .collect(Collectors.toList());

                List<RowType.Field> overideFields = new ArrayList(fields.size());
                for (RowType.Field field : fields) {
                    Type overideType = projectionTypes.get(field.getName().get());
                    if (overideType != null) {
                        overideFields.add(new RowType.Field(field.getName(), overideType));
                    }
                    else {
                        overideFields.add(field);
                    }
                }
                return symbolAllocator.newSymbol(oriSymbol.getName(), new ArrayType(RowType.from(overideFields)));
            }
        }
        return oriSymbol;
    }

    private Symbol getSymbolName(Optional<BiMap<Symbol, String>> symbolToOriginalName, String newName)
    {
        if (symbolToOriginalName.isPresent()) {
            BiMap<String, Symbol> inverse = symbolToOriginalName.get().inverse();
            return inverse.get(newName);
        }
        else {
            return new Symbol(newName);
        }
    }

    protected ListMultimap<Symbol, Symbol> trimNotRowInArray(ListMultimap<Symbol, Symbol> usedUnnestSymbols, TypeProvider tp)
    {
        ListMultimap<Symbol, Symbol> pruned = MultimapBuilder.treeKeys().arrayListValues().build();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : usedUnnestSymbols.asMap().entrySet()) {
            if (isRowInArray(tp.get(entry.getKey()))) {
                pruned.putAll(entry.getKey(), entry.getValue());
            }
        }
        return pruned;
    }

    protected boolean isRowInArray(Type type)
    {
        if (type instanceof ArrayType) {
            return ((ArrayType) type).getElementType() instanceof RowType;
        }
        return false;
    }

    private List<Symbol> replaceSymbolList(List<Symbol> unnestSymbols, Map<String, Symbol> replaceSymbols)
    {
        if (replaceSymbols.isEmpty()) {
            return unnestSymbols;
        }
        List<Symbol> newUnnestSymbol = new ArrayList<>(unnestSymbols.size());
        for (Symbol symbol : unnestSymbols) {
            Symbol replaceSymbol = replaceSymbols.get(symbol.getName());
            if (replaceSymbol != null) {
                newUnnestSymbol.add(replaceSymbol);
            }
            else {
                newUnnestSymbol.add(symbol);
            }
        }
        return newUnnestSymbol;
    }

    protected Map<Symbol, List<Symbol>> mergeUnnestSymbols(UnnestNode unnestNode, Map<Symbol, SymbolSet> newSymbols, Map<String, Symbol> replaceSymbols)
    {
        ImmutableMap.Builder<Symbol, List<Symbol>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, List<Symbol>> unnestSymbol : unnestNode.getUnnestSymbols().entrySet()) {
            if (newSymbols.containsKey(unnestSymbol.getKey())) {
                SymbolSet symbolSet = newSymbols.get(unnestSymbol.getKey());
                builder.put(symbolSet.getSymbol(), replaceSymbolList(symbolSet.getSubSymbols(), replaceSymbols));
            }
            else {
                builder.put(unnestSymbol.getKey(), replaceSymbolList(unnestSymbol.getValue(), replaceSymbols));
            }
        }
        return builder.build();
    }

    protected static class SymbolSet
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
