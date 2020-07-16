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
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.commons.lang3.Range;

import javax.annotation.Nullable;
import java.util.stream.Stream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertTrue;

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

    static {
        // Initializing DatabaseDescriptor that contains Cassandra's general
        // configuration
        DatabaseDescriptor.clientInitialization(false);
    }

    public CassandraSSTableLoader(String ssTablePath, boolean lazyLoading) {
        super(ssTablePath);
        this.ssTablePath = ssTablePath;
        this.lazyLoading = lazyLoading;

        try {
            Descriptor descriptor = Descriptor.fromFilename(this.ssTablePath);
            this.metadata = this.getSSTableMetadata(descriptor);
            this.ssTableReader = SSTableReader.openNoValidation(descriptor, this.metadata);
            // Get the schema of the current SSTable
            this.actualSchema = this.convertSchema();
            this.computeRowCount();
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing Metadata and SStable partitions", "{0}",
                    this.ssTablePath);
            throw new RuntimeException(e);
        }
    }

    public CFMetaData getSSTableMetadata(Descriptor desc) throws Exception {
        if (!desc.version.storeRows())
            throw new RuntimeException("pre-3.0 SSTable is not supported");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
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
        assertTrue(type instanceof CQL3Type.Native);
        switch ((CQL3Type.Native) colDef.type.asCQL3Type()) {
            case BOOLEAN:
            case ASCII:
            case INET:
            case TEXT:
            case TIMEUUID:
            case UUID:
            case DURATION:
            case TIME:
            case DATE:
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
            case TIMESTAMP:
                kind = ContentsKind.Date;
                break;
            case EMPTY:
            default:
                throw new RuntimeException("Unhandled column type " + type.toString());
        }
        return new ColumnDescription(name, kind);
    }

    public String toString() {
        CFMetaData metadata = this.metadata;
        StringBuilder sb = new StringBuilder("SSTable path: " + ssTablePath + "\n");
        sb.append("Column size : " + metadata.partitionColumns().size() + "\n");
        PartitionColumns columnDefinitions = metadata.partitionColumns();
        for (ColumnDefinition colDef : columnDefinitions) {
            sb.append(colDef.toString() + " " + colDef.type.asCQL3Type() + "\n");
        }
        return sb.toString();
    }

    public Schema convertSchema() {
        try {
            Schema result = new Schema();
            PartitionColumns columnDefinitions = this.metadata.partitionColumns();
            for (ColumnDefinition colDef : columnDefinitions) {
                ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                result.append(cd);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSchema() {
        return this.actualSchema;
    }

    public class SSTableColumnLoader implements IColumnLoader {
        private final SSTableReader ssTableReader;
        private final Schema actualSchema;
        private int columnCountToLoad;

        SSTableColumnLoader(SSTableReader ssTableReader, Schema actualSchema) {
            this.ssTableReader = ssTableReader;
            this.actualSchema = actualSchema;
        }

        // Marking the corresponding column index that are included in List<String>
        // names
        private boolean[] getColumnMarker(List<String> names) {
            this.columnCountToLoad = 0;
            List<String> colNames = this.actualSchema.getColumnNames();
            boolean[] result = new boolean[colNames.size()];
            int i = 0;
            for (String cn : colNames) {
                if (names.contains(cn)) {
                    result[i] = true;
                    // Incrementing the number of column that will be loaded
                    this.columnCountToLoad++;
                } else {
                    result[i] = false;
                }
                i++;
            }
            return result;
        }

        @Override
        public List<? extends IColumn> loadColumns(List<String> names) {
            boolean[] columnToLoad = this.getColumnMarker(names);
            List<IAppendableColumn> columns = createColumns(columnToLoad, this.columnCountToLoad);
            CassandraSSTableLoader.loadColumns(this.ssTableReader, columns, columnToLoad);
            return columns;
        }
    }

    public void computeRowCount() {
        ISSTableScanner currentScanner = this.ssTableReader.getScanner();
        Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner,
                Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);
        // Iterating each item inside the table
        partitions.forEach(partition -> {
            while (partition.hasNext()) {
                Unfiltered unfiltered = partition.next();
                if (unfiltered instanceof Row) {
                    this.rowCount++;
                } // else tombstone marker
            }
        });
    }

    public int getRowCount() {
        if (this.rowCount > Integer.MAX_VALUE || this.rowCount < Integer.MIN_VALUE)
            throw new RuntimeException("The number of rows exceeds Integer.MAX_VALUE");
        return (int) this.rowCount;
    }

    /** Instead of checking the columnn' name to find which one to load,
     * this method uses boolean marker stored at columnToLoad to recognize the needed columns */
    private List<IAppendableColumn> createColumns(boolean[] columnToLoad, int columnCountToLoad) {
        List<ColumnDescription> cols = Converters.checkNull(this.actualSchema).getColumnDescriptions();
        List<IAppendableColumn> result = new ArrayList<IAppendableColumn>(columnCountToLoad);
        int i = 0;
        for (ColumnDescription cd : cols) {
            if (columnToLoad[i])
                result.add(BaseListColumn.create(cd));
            i++;
        }
        return result;
    }

    /** This is the general column loader that can be triggered once the initialization is complete */
    public ITable load() {
        try {
            if (this.lazyLoading) {
                PartitionColumns columnDefinitions = metadata.partitionColumns();
                List<ColumnDescription> cds = new ArrayList<ColumnDescription>(columnDefinitions.size());
                for (ColumnDefinition colDef : columnDefinitions) {
                    ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                    cds.add(cd);
                }
                assert this.actualSchema != null;
                IColumnLoader loader = new CassandraSSTableLoader.SSTableColumnLoader(this.ssTableReader,
                    this.actualSchema);
                return Table.createLazyTable(cds, this.getRowCount(), this.filename, loader);
            } else {
                int columnCountToLoad = Converters.checkNull(this.actualSchema).getColumnCount();
                // columns loader will load all column, so all item of columnToLoad need to be TRUE
                boolean[] columnToLoad = new boolean[columnCountToLoad];
                Arrays.fill(columnToLoad, Boolean.TRUE);
                List<IAppendableColumn> columns = createColumns(columnToLoad, columnToLoad.length);
                CassandraSSTableLoader.loadColumns(this.ssTableReader, columns, columnToLoad);
                return new Table(columns, this.ssTablePath, null);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Lazy and non-lazy loader will call this function to load specific column marked by columnToLoad */
    private static void loadColumns(SSTableReader ssTableReader, List<IAppendableColumn> columns,
            boolean[] columnToLoad) {
        ISSTableScanner currentScanner = ssTableReader.getScanner();
        Spliterators.spliteratorUnknownSize(currentScanner,
        Spliterator.CONCURRENT).forEachRemaining( (partition) -> {
            Unfiltered unfiltered = partition.next();
            if (unfiltered instanceof Row) {
                Row row = (Row) unfiltered;
                int i = 0;
                int currentColumn = 0;
                Object value;
                IAppendableColumn col;
                // Extracting the value for each column from a single row
                for (ColumnData cd : row) {
                    // Filter the column to only load the one marked true by columnToLoad[]
                    if (columnToLoad[i]) {
                        if (cd.column().isSimple()) {
                            col = columns.get(currentColumn);
                            AbstractType<?> cellType = cd.column().cellValueType();
                            value = cellType.getSerializer().deserialize(((Cell) cd).value());
                            if (value == null) {
                                col.appendMissing();
                            } else {
                                CQL3Type colType = cd.column().type.asCQL3Type();
                                assertTrue(colType instanceof CQL3Type.Native);
                                switch ((CQL3Type.Native) colType) {
                                    case BOOLEAN:
                                    case ASCII:
                                    case INET:
                                    case TEXT:
                                    case TIMEUUID:
                                    case UUID:
                                    case DURATION:
                                    case VARCHAR:
                                        col.append(value.toString());
                                        break;
                                    case DATE:
                                    case TIME:
                                    case BLOB:
                                        col.append(cellType.getSerializer()
                                                .toCQLLiteral(((Cell) cd).value()).toString());
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
                                        col.append(((BigInteger) value).doubleValue());
                                        break;
                                    case BIGINT:
                                        col.append(((Long) value).doubleValue());
                                        break;
                                    case DECIMAL:
                                        if (bigDecimalInRange((BigDecimal) value))
                                            col.append(((BigDecimal) value).doubleValue());
                                        break;
                                    case DOUBLE:
                                        col.append((Double) value);
                                        break;
                                    case FLOAT:
                                        col.append(((Float) value).doubleValue());
                                        break;
                                    case COUNTER:
                                        col.append(CounterContext.instance().total(((Cell) cd).value()));
                                        break;
                                    case TIMESTAMP:
                                        col.append(((Date) value).toInstant());
                                        break;
                                    case EMPTY:
                                    default:
                                        throw new RuntimeException("Unhandled column type " + colType.toString());
                                }
                            }
                        } else {
                            // Hillview won't process the complex data
                            throw new RuntimeException("Hillview can't convert complex data");
                        }
                        currentColumn++;
                    }
                    i++;
                }
            }
        });;
    }

    private static boolean bigDecimalInRange(BigDecimal bigDecimal) {
        Range<BigDecimal> doubleRange = Range.between(BigDecimal.valueOf(Double.MIN_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE));
        if (!doubleRange.contains(bigDecimal)) {
            throw new RuntimeException("BigDecimal value is outside the range of Double");
        }
        return true;
    }
}