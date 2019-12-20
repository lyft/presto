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
package io.prestosql.sql.planner.sanity;

import com.google.common.base.VerifyException;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableProperties;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.isQueryPartitionFilterRequired;

public class PartitionFilterChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, final Session session, final Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        if (!isQueryPartitionFilterRequired(session)) {
            return;
        }
        plan.accept(new PlanVisitor<Void, Void>()
        {
            @Override
            protected Void visitPlan(PlanNode node, Void context)
            {
                for (PlanNode source : node.getSources()) {
                    source.accept(this, context);
                }
                return null;
            }

            @Override
            public Void visitTableScan(TableScanNode node, Void context)
            {
                TableHandle table = node.getTable();
                List<ColumnHandle> partitionColumns = getPartitionColumns(table);
                if (!partitionColumns.isEmpty() && node.getEnforcedConstraint().isAll()) {
                    String partitionColumnNames = partitionColumns.stream().map(n -> metadata.getColumnMetadata(session, table, n).getName()).collect(Collectors.joining(","));
                    throw new VerifyException("Filter on partition column required: " + partitionColumnNames);
                }
                return null;
            }

            private List<ColumnHandle> getPartitionColumns(TableHandle handle)
            {
                TableProperties properties = metadata.getTableProperties(session, handle);
                if (properties.getTablePartitioning().isPresent()
                        && !properties.getTablePartitioning().get().getPartitioningColumns().isEmpty()) {
                    return properties.getTablePartitioning().get().getPartitioningColumns();
                }
                if (properties.getDiscretePredicates().isPresent()
                                && !properties.getDiscretePredicates().get().getColumns().isEmpty()) {
                    return properties.getDiscretePredicates().get().getColumns();
                }
                return Collections.emptyList();
            }
        }, null);
    }
}
