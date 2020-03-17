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
package io.prestosql.plugin.thrift.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.thrift.api.PrestoThriftBlock;
import io.prestosql.plugin.thrift.api.PrestoThriftId;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableToken;
import io.prestosql.plugin.thrift.api.PrestoThriftPageResult;
import io.prestosql.plugin.thrift.api.PrestoThriftSchemaTableName;
import io.prestosql.plugin.thrift.api.PrestoThriftServiceException;
import io.prestosql.plugin.thrift.api.PrestoThriftSplit;
import io.prestosql.plugin.thrift.api.PrestoThriftSplitBatch;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import io.prestosql.split.MappedRecordSet;
import io.prestosql.testing.tpch.TpchIndexedData;
import io.prestosql.testing.tpch.TpchIndexedData.IndexedTable;
import io.prestosql.testing.tpch.TpchScaledTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.thrift.server.SplitInfo.indexSplit;
import static io.prestosql.testing.AbstractTestIndexedQueries.INDEX_SPEC;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ThriftIndexedTpchService
        extends ThriftTpchService
{
    private static final int NUMBER_OF_INDEX_SPLITS = 2;
    private final TpchIndexedData indexedData = new TpchIndexedData(INDEX_SPEC);

    @Override
    protected List<Set<String>> getIndexableKeys(String schemaName, String tableName)
    {
        TpchScaledTable tpchScaledTable = new TpchScaledTable(tableName, schemaNameToScaleFactor(schemaName));
        return ImmutableList.copyOf(INDEX_SPEC.getColumnIndexes(tpchScaledTable));
    }

    @Override
    protected PrestoThriftSplitBatch getIndexSplitsSync(
            PrestoThriftSchemaTableName schemaTableName,
            List<String> indexColumnNames,
            PrestoThriftPageResult keys,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
            throws PrestoThriftServiceException
    {
        checkArgument(NUMBER_OF_INDEX_SPLITS <= maxSplitCount, "maxSplitCount for lookup splits is too low");
        checkArgument(nextToken.getToken() == null, "no continuation is supported for lookup splits");
        int totalKeys = keys.getRowCount();
        int partSize = totalKeys / NUMBER_OF_INDEX_SPLITS;
        List<PrestoThriftSplit> splits = new ArrayList<>(NUMBER_OF_INDEX_SPLITS);
        for (int splitIndex = 0; splitIndex < NUMBER_OF_INDEX_SPLITS; splitIndex++) {
            int begin = partSize * splitIndex;
            int end = partSize * (splitIndex + 1);
            if (splitIndex + 1 == NUMBER_OF_INDEX_SPLITS) {
                // add remainder to the last split
                end = totalKeys;
            }
            if (begin == end) {
                // split is empty, skip it
                continue;
            }
            SplitInfo splitInfo = indexSplit(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    indexColumnNames,
                    thriftPageToList(keys, begin, end));
            splits.add(new PrestoThriftSplit(new PrestoThriftId(SPLIT_INFO_CODEC.toJsonBytes(splitInfo)), ImmutableList.of()));
        }
        return new PrestoThriftSplitBatch(splits, null);
    }

    @Override
    protected ConnectorPageSource createLookupPageSource(SplitInfo splitInfo, List<String> outputColumnNames)
    {
        IndexedTable indexedTable = indexedData.getIndexedTable(
                splitInfo.getTableName(),
                schemaNameToScaleFactor(splitInfo.getSchemaName()),
                ImmutableSet.copyOf(splitInfo.getLookupColumnNames()))
                .orElseThrow(() -> new IllegalArgumentException(format("No such index: %s%s", splitInfo.getTableName(), splitInfo.getLookupColumnNames())));
        List<Type> lookupColumnTypes = types(splitInfo.getTableName(), splitInfo.getLookupColumnNames());
        RecordSet keyRecordSet = new ListBasedRecordSet(splitInfo.getKeys(), lookupColumnTypes);
        RecordSet outputRecordSet = lookupIndexKeys(keyRecordSet, indexedTable, outputColumnNames, splitInfo.getLookupColumnNames());
        return new RecordPageSource(outputRecordSet);
    }

    /**
     * Get lookup result and re-map output columns based on requested order.
     */
    private static RecordSet lookupIndexKeys(RecordSet keys, IndexedTable table, List<String> outputColumnNames, List<String> lookupColumnNames)
    {
        RecordSet allColumnsOutputRecordSet = table.lookupKeys(
                new MappedRecordSet(
                        keys,
                        computeRemap(lookupColumnNames, table.getKeyColumns())));
        List<Integer> outputRemap = computeRemap(table.getOutputColumns(), outputColumnNames);
        return new MappedRecordSet(allColumnsOutputRecordSet, outputRemap);
    }

    private static List<List<String>> thriftPageToList(PrestoThriftPageResult page, int begin, int end)
    {
        checkArgument(begin <= end, "invalid interval");
        if (begin == end) {
            // empty interval
            return ImmutableList.of();
        }
        List<PrestoThriftBlock> blocks = page.getColumnBlocks();
        List<List<String>> result = new ArrayList<>(blocks.size());
        for (PrestoThriftBlock block : blocks) {
            result.add(blockAsList(block, begin, end));
        }
        return result;
    }

    private static List<String> blockAsList(PrestoThriftBlock block, int begin, int end)
    {
        List<String> result = new ArrayList<>(end - begin);
        if (block.getBigintData() != null) {
            boolean[] nulls = block.getBigintData().getNulls();
            long[] longs = block.getBigintData().getLongs();
            for (int index = begin; index < end; index++) {
                if (nulls != null && nulls[index]) {
                    result.add(null);
                }
                else {
                    checkArgument(longs != null, "block structure is incorrect");
                    result.add(String.valueOf(longs[index]));
                }
            }
        }
        else if (block.getIntegerData() != null) {
            boolean[] nulls = block.getIntegerData().getNulls();
            int[] ints = block.getIntegerData().getInts();
            for (int index = begin; index < end; index++) {
                if (nulls != null && nulls[index]) {
                    result.add(null);
                }
                else {
                    checkArgument(ints != null, "block structure is incorrect");
                    result.add(String.valueOf(ints[index]));
                }
            }
        }
        else if (block.getVarcharData() != null) {
            boolean[] nulls = block.getVarcharData().getNulls();
            int[] sizes = block.getVarcharData().getSizes();
            byte[] bytes = block.getVarcharData().getBytes();
            int startOffset = 0;
            // calculate cumulative offset before the starting position
            if (sizes != null) {
                for (int index = 0; index < begin; index++) {
                    if (nulls == null || !nulls[index]) {
                        startOffset += sizes[index];
                    }
                }
            }
            for (int index = begin; index < end; index++) {
                if (nulls != null && nulls[index]) {
                    result.add(null);
                }
                else {
                    checkArgument(sizes != null, "block structure is incorrect");
                    if (sizes[index] == 0) {
                        result.add("");
                    }
                    else {
                        checkArgument(bytes != null);
                        result.add(new String(bytes, startOffset, sizes[index], UTF_8));
                        startOffset += sizes[index];
                    }
                }
            }
        }
        else {
            throw new IllegalArgumentException("Only bigint, integer and varchar blocks are supported");
        }
        return result;
    }

    private static List<Integer> computeRemap(List<String> startSchema, List<String> endSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String columnName : endSchema) {
            int index = startSchema.indexOf(columnName);
            checkArgument(index != -1, "Column name in end that is not in the start: %s", columnName);
            builder.add(index);
        }
        return builder.build();
    }
}
