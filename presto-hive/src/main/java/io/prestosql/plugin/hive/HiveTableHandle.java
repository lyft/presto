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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveBucketing.HiveBucketFilter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HiveTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<HiveColumnHandle> partitionColumns;
    private final Optional<List<HivePartition>> partitions;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final Optional<List<List<String>>> analyzePartitionValues;
    private final Optional<Constraint> partitionConstraint;

    @JsonCreator
    public HiveTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            @JsonProperty("enforcedConstraint") TupleDomain<ColumnHandle> enforcedConstraint,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketFilter> bucketFilter,
            @JsonProperty("analyzePartitionValues") Optional<List<List<String>>> analyzePartitionValues)
    {
        this(schemaName,
                tableName,
                partitionColumns,
                Optional.empty(),
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues);
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> partitionColumns,
            Optional<HiveBucketHandle> bucketHandle)
    {
        this(schemaName,
                tableName,
                partitionColumns,
                Optional.empty(),
                TupleDomain.all(),
                TupleDomain.all(),
                bucketHandle,
                Optional.empty(),
                Optional.empty());
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> partitionColumns,
            Optional<List<HivePartition>> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            Optional<List<List<String>>> analyzePartitionValues)
    {
        this(schemaName, tableName, partitionColumns, partitions, compactEffectivePredicate, enforcedConstraint, bucketHandle, bucketFilter, analyzePartitionValues, Optional.empty());
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> partitionColumns,
            Optional<List<HivePartition>> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            Optional<List<List<String>>> analyzePartitionValues,
            Optional<Constraint> partitionConstraint)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.analyzePartitionValues = requireNonNull(analyzePartitionValues, "analyzePartitionValues is null");
        this.partitionConstraint = partitionConstraint;
    }

    public HiveTableHandle withAnalyzePartitionValues(Optional<List<List<String>>> analyzePartitionValues)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                partitionColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    // do not serialize partitions as they are not needed on workers
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    @JsonProperty
    public Optional<HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }

    @JsonProperty
    public Optional<List<List<String>>> getAnalyzePartitionValues()
    {
        return analyzePartitionValues;
    }

    // do not serialize partition constraint as it is not needed on workers
    @JsonIgnore
    public Optional<Constraint> getPartitionConstraint()
    {
        return partitionConstraint;
    }

    @Override
    public void validateScan(ConnectorSession session)
    {
        if (HiveSessionProperties.isQueryPartitionFilterRequired(session) && !partitionColumns.isEmpty()
                && getEnforcedConstraint().isAll()
                && (!getPartitionConstraint().isPresent() || !getPartitionConstraint().get().predicate().isPresent())) {
            String partitionColumnNames = partitionColumns.stream().map(n -> n.getName()).collect(Collectors.joining(","));
            throw new PrestoException(
                    StandardErrorCode.QUERY_REJECTED,
                    String.format("Filter required on %s.%s for at least one partition column: %s ", schemaName, tableName, partitionColumnNames));
        }
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveTableHandle that = (HiveTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaName).append(":").append(tableName);
        bucketHandle.ifPresent(bucket ->
                builder.append(" bucket=").append(bucket.getReadBucketCount()));
        return builder.toString();
    }
}
