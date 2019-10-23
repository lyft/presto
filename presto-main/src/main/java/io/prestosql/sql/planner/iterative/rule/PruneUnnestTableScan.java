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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.planner.plan.Patterns.unnest;

public class PruneUnnestTableScan
        extends PruneUnusedNestedColumns
{
    private static final Capture<UnnestNode> UNNEST = newCapture();
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(unnest().capturedAs(UNNEST)
                    .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

    public PruneUnnestTableScan(Metadata metadata)
    {
        super(metadata);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
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

    private Map<Symbol, RowType.Field> getOriginalName(UnnestNode unnestNode, Symbol column, List<RowType.Field> nestedTypes)
    {
        List<Symbol> symbols = unnestNode.getUnnestSymbols().get(column);
        ImmutableMap.Builder<Symbol, RowType.Field> builder = ImmutableMap.builder();
        for (int i = 0; i < symbols.size(); i++) {
            builder.put(symbols.get(i), nestedTypes.get(i));
        }
        return builder.build();
    }

    private Map<String, List<String>> transform(ListMultimap<Symbol, Symbol> original, Optional<BiMap<Symbol, String>> mapBetweenSymbolAndTypeName)
    {
        ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : original.asMap().entrySet()) {
            ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
            for (Symbol s : entry.getValue()) {
                listBuilder.add(mapBetweenSymbolAndTypeName.get().get(s));
            }

            builder.put(mapBetweenSymbolAndTypeName.get().get(entry.getKey()), listBuilder.build());
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

        Optional<BiMap<Symbol, String>> mapBetweenSymbolAndTypeName = Optional.of(getOriginalName(tableScanNode, unnestNode, usedUnnestSymbols, context.getSession(), tableScanNode.getTable()));

        Map<String, Symbol> replaceSymbols = Maps.newHashMapWithExpectedSize(parent.getAssignments().getMap().keySet().size());
        Map<String, Type> projectionTypes = Maps.newHashMapWithExpectedSize(parent.getAssignments().getMap().keySet().size());
        Assignments.Builder newProjectAssignmentBuilder = Assignments.builder();
        for (Map.Entry<Symbol, Expression> entry : parent.getAssignments().getMap().entrySet()) {
            Type type = context.getSymbolAllocator().getTypes().get(entry.getKey());
            projectionTypes.put(((SymbolReference) entry.getValue()).getName(), type);
            replaceSymbols.put(((SymbolReference) entry.getValue()).getName(), entry.getKey());
            newProjectAssignmentBuilder.putIdentity(entry.getKey());
        }
        Assignments newProjectAssignments = newProjectAssignmentBuilder.build();
        // Get new ColumnHandles using the Metadata API
        Map<String, ColumnHandle> prunedColumnHandles = metadata.getNestedColumnHandles(context.getSession(), tableScanNode.getTable(), transform(usedUnnestSymbols, mapBetweenSymbolAndTypeName), projectionTypes);
        if (prunedColumnHandles.isEmpty()) {
            return Result.empty();
        }

        // Generate new Symbols
        Map<Symbol, SymbolSet> newSymbols = generateNewSymbols(context.getSymbolAllocator().getTypes(), context.getSymbolAllocator(), unnestNode, usedUnnestSymbols, mapBetweenSymbolAndTypeName, projectionTypes);

        // Rewrite assignments in tableScanNode
        Map<Symbol, ColumnHandle> assignments = tableScanNode.getAssignments();
        ImmutableMap.Builder<Symbol, ColumnHandle> newAssignmentBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            if (newSymbols.containsKey(entry.getKey())) {
                Symbol newSymbol = newSymbols.get(entry.getKey()).getSymbol();
                String typeName = mapBetweenSymbolAndTypeName.get().get(entry.getKey());
                ColumnHandle columnHandle = prunedColumnHandles.get(typeName);
                newAssignmentBuilder.put(
                        newSymbol,
                        columnHandle);
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
        Map<Symbol, List<Symbol>> symbolMap = mergeUnnestSymbols(unnestNode, newSymbols, replaceSymbols);
        PlanNode newUnnestNode = new UnnestNode(
                context.getIdAllocator().getNextId(),
                newTableScanNode,
                unnestNode.getReplicateSymbols(),
                symbolMap,
                unnestNode.getOrdinalitySymbol());
        ProjectNode newProjectNode = new ProjectNode(context.getIdAllocator().getNextId(), newUnnestNode, newProjectAssignments);

        return Result.ofPlanNode(newProjectNode);
    }
}
