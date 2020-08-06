/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
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

package org.hillview.storage;

import org.hillview.table.api.*;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.storage.CassandraDatabase.CassandraTokenRange;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.DurationSerializer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import java.util.stream.Stream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Knows how to read a local Cassandra's SSTable file.
 */
public class CassandraSSTableLoader extends TextFileLoader {
    @Nullable
    private final Schema actualSchema;
    private final String ssTablePath;
    private long rowCount;

    public boolean lazyLoading;
    public final CFMetaData metadata;
    private final SSTableReader ssTableReader;

    private final PartitionColumns columnDefinitions;
    private final IPartitioner partitioner;
    private final List<CassandraTokenRange> tokenRanges;
    private final String localEndpoint;
    private static final org.apache.commons.lang3.Range<BigDecimal> doubleRange = org.apache.commons.lang3.Range
            .between(BigDecimal.valueOf(Double.MIN_VALUE), BigDecimal.valueOf(Double.MAX_VALUE));

    static {
        // Initializing Cassandra's general configuration
        DatabaseDescriptor.clientInitialization(false);
    }

    public CassandraSSTableLoader(String ssTablePath, List<CassandraTokenRange> tokenRanges, String localEndpoint,
            boolean lazyLoading) {
        super(ssTablePath);
        this.ssTablePath = ssTablePath;
        this.tokenRanges = tokenRanges;
        this.localEndpoint = localEndpoint;
        this.lazyLoading = lazyLoading;

        try {
            Descriptor descriptor = Descriptor.fromFilename(this.ssTablePath);

            this.partitioner = FBUtilities.newPartitioner(descriptor);
            this.metadata = this.getSSTableMetadata(descriptor);
            this.columnDefinitions = this.metadata.partitionColumns();
            this.ssTableReader = SSTableReader.openNoValidation(descriptor, this.metadata);
            this.actualSchema = this.convertSchema();
            this.computeRowCount();
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing Metadata and SStable partitions", "{0}",
                    this.ssTablePath);
            throw new RuntimeException(e);
        }
    }

    /**
     * This function is only used for testing. This will support loading local
     * sstable test without a token-range. The generated token range will cover all
     * partition in a single range.
     */
    @VisibleForTesting
    public static CassandraSSTableLoader getTestTableLoader(String ssTablePath, boolean lazyLoading) {
        try {
            Descriptor descriptor = Descriptor.fromFilename(ssTablePath);
            IPartitioner partitioner = FBUtilities.newPartitioner(descriptor);
            List<String> endpoints = new ArrayList<>();
            List<CassandraTokenRange> tokenRanges = new ArrayList<>();
            endpoints.add("127.0.0.1");
            tokenRanges.add(
                    new CassandraTokenRange(partitioner.getMinimumToken(), partitioner.getMaximumToken(), endpoints));
            return new CassandraSSTableLoader(ssTablePath, tokenRanges, "127.0.0.1", lazyLoading);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CFMetaData getSSTableMetadata(Descriptor desc) throws Exception {
        if (!desc.version.storeRows())
            throw new RuntimeException("pre-3.0 SSTable is not supported");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(this.partitioner);
        header.getStaticColumns().forEach((key, value) -> {
            ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(key), true);
            builder.addStaticColumn(ident, value);
        });
        header.getRegularColumns().forEach((key, value) -> {
            ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(key), true);
            builder.addRegularColumn(ident, value);
        });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++) {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    /** Converting Cassandra's data type to Hillview data type */
    private static ColumnDescription getDescription(ColumnDefinition colDef) {
        String name = colDef.toString();
        ContentsKind kind;
        CQL3Type type = colDef.type.asCQL3Type();
        switch ((CQL3Type.Native) colDef.type.asCQL3Type()) {
            case BOOLEAN:
            case ASCII:
            case INET:
            case TEXT:
            case TIMEUUID:
            case UUID:
            case BLOB:
            case VARCHAR:
                kind = ContentsKind.String;
                break;
            case INT:
            case SMALLINT:
            case TINYINT:
                kind = ContentsKind.Integer;
                break;
            case VARINT:
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case COUNTER:
                kind = ContentsKind.Double;
                break;
            case TIME:
            case DURATION:
                kind = ContentsKind.Duration;
                break;
            case TIMESTAMP:
            case DATE:
                kind = ContentsKind.Date;
                break;
            case EMPTY:
                kind = ContentsKind.None;
                break;
            default:
                throw new RuntimeException("Unhandled column type " + type.toString());
        }
        return new ColumnDescription(name, kind);
    }

    public String toString() {
        CFMetaData metadata = this.metadata;
        StringBuilder sb = new StringBuilder("SSTable path: " + ssTablePath + "\n");
        sb.append("Column size : ").append(metadata.partitionColumns().size()).append("\n");
        for (ColumnDefinition colDef : this.columnDefinitions) {
            sb.append(colDef.toString()).append(" ").append(colDef.type.asCQL3Type()).append("\n");
        }
        return sb.toString();
    }

    public Schema convertSchema() {
        try {
            Schema result = new Schema();
            for (ColumnDefinition colDef : this.columnDefinitions) {
                if (colDef.isComplex())
                    // Hillview won't process the complex data
                    throw new RuntimeException("Hillview can't convert complex column");
                ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                result.append(cd);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSchema() {
        return Converters.checkNull(this.actualSchema);
    }

    private static SSTableReadsListener newReadCountUpdater() {
        return new SSTableReadsListener() {
            @Override
            public void onScanningStarted(SSTableReader sstable) {
                // No need to do sstable.incrementReadCount();
            }
        };
    }

    private void computeRowCount() {
        this.rowCount = 0;
        PartitionColumns.Builder builder = PartitionColumns.builder();
        ColumnFilter cf = ColumnFilter.selection(builder.build());

        for (CassandraTokenRange tr : this.tokenRanges) {
            // Load the partition if the local endpoint is listed as the first endpoint
            if (tr.endpoints.get(0).equals(this.localEndpoint)) {
                DataRange range = DataRange.forTokenRange(tr.tokenRange);
                SSTableReadsListener listener = CassandraSSTableLoader.newReadCountUpdater();
                ISSTableScanner currentScanner = this.ssTableReader.getScanner(cf, range, false, listener);
                Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner,
                        Spliterator.IMMUTABLE);
                Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false)
                        .filter(Iterator::hasNext).filter(r -> r.next() instanceof Row);
                this.rowCount += partitions.count();
            }
        }
    }

    public int getRowCount() {
        return Converters.toInt(this.rowCount);
    }

    private List<IAppendableColumn> createColumns(List<String> names) {
        Set<String> namesSet = new HashSet<String>(names);
        List<ColumnDescription> cols = Converters.checkNull(this.actualSchema).getColumnDescriptions();
        List<IAppendableColumn> result = new ArrayList<IAppendableColumn>(names.size());
        for (ColumnDescription cd : cols) {
            if (namesSet.contains(cd.name))
                result.add(BaseListColumn.create(cd));
        }
        return result;
    }

    public class SSTableColumnLoader implements IColumnLoader {
        private final SSTableReader ssTableReader;
        private final List<CassandraTokenRange> tokenRanges;
        private final PartitionColumns columnDefinitions;
        private final String localEndpoint;

        SSTableColumnLoader(SSTableReader ssTableReader, PartitionColumns columnDefinitions,
                List<CassandraTokenRange> tokenRanges, String localEndpoint) {
            this.ssTableReader = ssTableReader;
            this.columnDefinitions = columnDefinitions;
            this.tokenRanges = tokenRanges;
            this.localEndpoint = localEndpoint;
        }

        @Override
        public List<? extends IColumn> loadColumns(List<String> names) {
            List<IAppendableColumn> columns = createColumns(names);
            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (ColumnDefinition c : columnDefinitions) {
                if (names.contains(c.toString()))
                    builder.add(c);
            }
            PartitionColumns newColDef = builder.build();
            CassandraSSTableLoader.loadColumns(this.ssTableReader, columns, newColDef, this.tokenRanges,
                    this.localEndpoint);
            return columns;
        }
    }

    /**
     * This is the general column loader that can be triggered once the
     * initialization is complete
     */
    public ITable load() {
        try {
            Converters.checkNull(this.actualSchema);
            if (this.lazyLoading) {
                List<ColumnDescription> cds = new ArrayList<ColumnDescription>(this.columnDefinitions.size());
                for (ColumnDefinition colDef : this.columnDefinitions) {
                    ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                    cds.add(cd);
                }
                IColumnLoader loader = new CassandraSSTableLoader.SSTableColumnLoader(this.ssTableReader,
                        this.columnDefinitions, this.tokenRanges, this.localEndpoint);
                return Table.createLazyTable(cds, this.getRowCount(), this.filename, loader);
            } else {
                List<IAppendableColumn> columns = createColumns(
                        Converters.checkNull(this.actualSchema.getColumnNames()));
                CassandraSSTableLoader.loadColumns(this.ssTableReader, columns, this.columnDefinitions,
                        this.tokenRanges, this.localEndpoint);
                return new Table(columns, this.ssTablePath, null);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * This function will load specific column listed at columnDefinitions and in certain token ranges 
     */
    private static void loadColumns(SSTableReader ssTableReader, List<IAppendableColumn> columns,
            PartitionColumns columnDefinitions, List<CassandraTokenRange> tokenRanges, String localEndpoint) {
        ColumnFilter cf = ColumnFilter.selection(columnDefinitions);
        TypeSerializer<?>[] arrSerializers = new TypeSerializer<?>[columnDefinitions.size()];
        CQL3Type[] arrColTypes = new CQL3Type[columnDefinitions.size()];
        Iterator<ColumnDefinition> colIter = columnDefinitions.regulars.simpleColumns();
        int i = 0;
        // init reusable serializer and prefixComparison
        while (colIter.hasNext()) {
            ColumnDefinition cd = colIter.next();
            CQL3Type colType = cd.type.asCQL3Type();
            arrColTypes[i] = colType;
            arrSerializers[i] = colType.getType().getSerializer();
            i++;
        }

        for (CassandraTokenRange tr : tokenRanges) {
            // Load the partition if the local endpoint is listed as the first endpoint
            if (!tr.endpoints.get(0).equals(localEndpoint)) continue;
            DataRange range = DataRange.forTokenRange(tr.tokenRange);
            SSTableReadsListener listener = CassandraSSTableLoader.newReadCountUpdater();
            ISSTableScanner currentScanner = ssTableReader.getScanner(cf, range, false, listener);
            Spliterators.spliteratorUnknownSize(currentScanner, Spliterator.CONCURRENT)
                .forEachRemaining((partition) -> {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered instanceof Row) {
                        Row row = (Row) unfiltered;
                        int currentColumn = 0;
                        IAppendableColumn col;
                        // Extracting the value of each column from a single row
                        for (ColumnDefinition colDef : columnDefinitions) {
                            Cell cd = row.getCell(colDef);
                            col = columns.get(currentColumn);
                            ByteBuffer byteBuff = cd.value();
                            // The column that has null value can be identified by its buffer capacity
                            if (byteBuff.capacity() == 0) {
                                col.appendMissing(); 
                            } else {
                                Object value = arrSerializers[currentColumn].deserialize(byteBuff);
                                switch ((CQL3Type.Native) arrColTypes[currentColumn]) {
                                    case ASCII:
                                    case BOOLEAN:
                                    case INET:
                                    case TEXT:
                                    case TIMEUUID:
                                    case UUID:
                                    case VARCHAR:
                                        col.append(value.toString());
                                        break;
                                    case BLOB:
                                        col.append(arrSerializers[currentColumn].toCQLLiteral(byteBuff));
                                        break;
                                    case INT:
                                        col.append((Integer) value);
                                        break;
                                    case SMALLINT:
                                        col.append(((Short) value).intValue());
                                        break;
                                    case TINYINT:
                                        col.append(((Byte) value).intValue());
                                        break;
                                    case VARINT:
                                        col.append(((BigInteger)value).doubleValue());
                                        break;
                                    case BIGINT:
                                        col.append(((Long)value).doubleValue());
                                        break;
                                    case DECIMAL:
                                        col.append(bigDecimalToDouble((BigDecimal) value));
                                        break;
                                    case DOUBLE:
                                        col.append((Double)value);
                                        break;
                                    case FLOAT:
                                        col.append(((Float)value).doubleValue());
                                        break;
                                    case COUNTER:
                                        col.append(CounterContext.instance().total(byteBuff));
                                        break;
                                    case TIME:
                                        // Time is in nanoseconds; convert to duration
                                        col.append((double)((Long)value / 1000000));
                                        break;
                                    case TIMESTAMP:
                                        // This is the same as Converters.toDouble(((Date)value).toInstant())
                                        col.append(((Date)value).getTime());
                                        break;
                                    case DATE:
                                        // Same as Converters.toDouble(Converters.toDate(...))
                                        long msTime = SimpleDateSerializer.dayToTimeInMillis((Integer)value);
                                        col.append(msTime);
                                        break;
                                    case DURATION:
                                        Duration duration = DurationSerializer.instance.deserialize(byteBuff);
                                        if (duration.getMonths() == 0) {
                                            int days = duration.getDays();
                                            long nanos = duration.getNanoseconds();
                                            long millis = (nanos + Duration.NANOS_PER_HOUR * 24 * days) / 1000000;
                                            // This is the same as Converters.toDouble(Converters.toDuration(millis))
                                            col.append(millis);
                                        } else {
                                            // java.time.Duration support day and time (but not month and year)
                                            throw new RuntimeException(
                                                    "Cassandra Durations with months are not supported");
                                        }
                                        break;
                                    case EMPTY:
                                        break;
                                }
                            }
                            currentColumn++;
                        }
                    }
                });
        }
    }

    private static Double bigDecimalToDouble(BigDecimal bigDecimal) {
        if (!CassandraSSTableLoader.doubleRange.contains(bigDecimal))
            throw new RuntimeException("BigDecimal value is outside the range of Double");
        else 
            return bigDecimal.doubleValue();
    }
}
