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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.serializers.TypeSerializer;

import javax.annotation.Nullable;

import java.util.stream.Stream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;
import java.util.function.Consumer;

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
    private List<Consumer<Object>> arrConverters = new ArrayList<>();
    private List<ConvertedType> arrConvertedTypes = new ArrayList<>();
    private List< TypeSerializer<Object>> arrTypeSerializers = new ArrayList<>();

    // This variables will be used as a temporary storage of the converted values
    private static Double valueDouble;
    private static Integer valueInteger;
    private static Instant valueDate;
    private static Duration valueDuration;

    /** This enum is needed to identify the type of typeConverters's output */
    enum ConvertedType {
        String,
        CQLString,
        Double,
        Integer,
        Date,
        Duration,
        Counter
    }

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
            this.metadata = this.setSSTableMetadata(descriptor);
            this.ssTableReader = SSTableReader.openNoValidation(descriptor, this.metadata);
            // Get the schema of the current SSTable
            this.actualSchema = this.setSchema();
            this.setRowCount();
            this.setArrayConverters();
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing Metadata and SStable partitions", "{0}",
                    this.ssTablePath);
            throw new RuntimeException(e);
        }
    }

    public CFMetaData setSSTableMetadata(Descriptor desc) throws Exception {
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
        String colType = colDef.type.asCQL3Type().toString();
        ContentsKind kind;
        switch (colType) {
            case "boolean":
            case "ascii":
            case "inet":
            case "text":
            case "timeuuid":
            case "uuid":
            case "duration":
            case "time":
            case "date":
            case "blob":
                kind = ContentsKind.String;
                break;
            case "int":
            case "smallint":
            case "tinyint":
                kind = ContentsKind.Integer;
                break;
            case "varint":
            case "bigint":
            case "decimal":
            case "double":
            case "float":
            case "counter":
                kind = ContentsKind.Double;
                break;
            case "timestamp":
                kind = ContentsKind.Date;
                break;
            default:
                throw new RuntimeException("Unhandled column type " + colType);
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

    public Schema setSchema() {
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
            ISSTableScanner currentScanner = this.ssTableReader.getScanner();
            Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner,
                    Spliterator.IMMUTABLE);
            Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);

            return CassandraSSTableLoader.this.loadColumns(columnToLoad, partitions, this.columnCountToLoad);
        }
    }

    public void setRowCount() {
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
        if (this.rowCount > Integer.MAX_VALUE || this.rowCount < Integer.MIN_VALUE)
            throw new RuntimeException("The number of rows exceeds Integer.MAX_VALUE");
    }

    public int getRowCount(){
        return (int) this.rowCount;
    }

    private static void converterStub(Object value) {
        // Nothing to do here because the conversion will be done manually.
        // This method will be pushed to arrConverters to keep the id relevant when
        // adding the next converters.
    }

    private static void convertFloatToDouble(Object value) {
        CassandraSSTableLoader.valueDouble = ((Float) value).doubleValue();
    }

    private static void convertLongToDouble(Object value) {
        CassandraSSTableLoader.valueDouble = ((Long) value).doubleValue();
    }

    private static void convertBigIntToDouble(Object value) {
        CassandraSSTableLoader.valueDouble = ((BigInteger) value).doubleValue();
    }

    private static void convertDecimalToDouble(Object value) {
        CassandraSSTableLoader.valueDouble = ((BigDecimal) value).doubleValue();
    }

    private static void convertDoubleToDouble(Object value) {
        CassandraSSTableLoader.valueDouble = (Double) value;
    }

    private static void convertIntegerToInteger(Object value) {
        CassandraSSTableLoader.valueInteger = (Integer) value;
    }

    private static void convertSmalIntToInteger(Object value) {
        CassandraSSTableLoader.valueInteger = ((Short) value).intValue();
    }

    private static void convertByteToInteger(Object value) {
        CassandraSSTableLoader.valueInteger = ((Byte) value).intValue();
    }

    private static void convertTimestampToDate(Object value) {
        CassandraSSTableLoader.valueDate = ((Date) value).toInstant();
    }

    private void setArrayConverters() {
        PartitionColumns columnDefinitions = this.metadata.partitionColumns();
        for (ColumnDefinition colDef : columnDefinitions) {
            String colType = colDef.type.asCQL3Type().toString();
            switch (colType) {
                case "boolean":
                case "ascii":
                case "inet":
                case "text":
                case "timeuuid":
                case "duration":
                case "uuid":
                    this.arrConverters.add(CassandraSSTableLoader::converterStub);
                    this.arrConvertedTypes.add(ConvertedType.String);
                    break;
                case "smallint":
                    this.arrConverters.add(CassandraSSTableLoader::convertSmalIntToInteger);
                    this.arrConvertedTypes.add(ConvertedType.Integer);
                    break;
                case "tinyint":
                    this.arrConverters.add(CassandraSSTableLoader::convertByteToInteger);
                    this.arrConvertedTypes.add(ConvertedType.Integer);
                    break;
                case "int":
                    this.arrConverters.add(CassandraSSTableLoader::convertIntegerToInteger);
                    this.arrConvertedTypes.add(ConvertedType.Integer);
                    break;
                case "varint":
                    this.arrConverters.add(CassandraSSTableLoader::convertBigIntToDouble);
                    this.arrConvertedTypes.add(ConvertedType.Double);
                    break;
                case "bigint":
                    this.arrConverters.add(CassandraSSTableLoader::convertLongToDouble);
                    this.arrConvertedTypes.add(ConvertedType.Double);
                    break;
                case "decimal":
                    this.arrConverters.add(CassandraSSTableLoader::convertDecimalToDouble);
                    this.arrConvertedTypes.add(ConvertedType.Double);
                    break;
                case "double":
                    this.arrConverters.add(CassandraSSTableLoader::convertDoubleToDouble);
                    this.arrConvertedTypes.add(ConvertedType.Double);
                    break;
                case "float":
                    this.arrConverters.add(CassandraSSTableLoader::convertFloatToDouble);
                    this.arrConvertedTypes.add(ConvertedType.Double);
                    break;
                case "timestamp":
                    this.arrConverters.add(CassandraSSTableLoader::convertTimestampToDate);
                    this.arrConvertedTypes.add(ConvertedType.Date);
                    break;
                case "date":
                case "time":
                case "blob":
                    this.arrConverters.add(CassandraSSTableLoader::converterStub);
                    this.arrConvertedTypes.add(ConvertedType.CQLString);
                    break;
                case "counter":
                    this.arrConverters.add(CassandraSSTableLoader::converterStub);
                    this.arrConvertedTypes.add(ConvertedType.Counter);
                    break;
                default:
                    throw new RuntimeException("Unhandled column type " + colType);
            }
        }
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
                return Table.createLazyTable(cds, (int)this.rowCount, this.filename, loader);
            } else {
                int columnCountToLoad = Converters.checkNull(this.actualSchema).getColumnCount();
                // columns loader will load all column, so all item of columnToLoad need to be TRUE
                boolean[] columnToLoad = new boolean[columnCountToLoad];
                Arrays.fill(columnToLoad, Boolean.TRUE);

                ISSTableScanner currentScanner = this.ssTableReader.getScanner();
                Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner,
                    Spliterator.IMMUTABLE);
                Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);

                List<IAppendableColumn> arrColumns = this.loadColumns(columnToLoad, partitions, columnCountToLoad);
                return new Table(arrColumns, this.ssTablePath, null);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Lazy and non-lazy loader will call this function to load specific column marked by columnToLoad */
    private List<IAppendableColumn> loadColumns(boolean[] columnToLoad, Stream<UnfilteredRowIterator> partitions, int size) {
        List<IAppendableColumn> columns = createColumns(columnToLoad, size);
        // Iterating each item inside the table
        partitions.forEach(partition -> {
            while (partition.hasNext()) {
                Unfiltered unfiltered = partition.next();
                if (unfiltered instanceof Row) {
                    Row row = (Row) unfiltered;
                    int i = 0;
                    int currentColumn = 0;
                    Object value;
                    IAppendableColumn col;
                    // Initiate TypeSerializer array so that it can be reused to load all rows
                    if (this.arrTypeSerializers.size() == 0) {
                        for (ColumnData cd : row) {
                            if (cd.column().isSimple()) {
                                AbstractType<?> cellType = cd.column().cellValueType();
                                this.arrTypeSerializers.add((TypeSerializer<Object>) cellType.getSerializer());
                            }
                        }
                    }
                    // Extracting the value for each column from a single row
                    for (ColumnData cd : row) {
                        // Filter the column to only load the one marked true by columnToLoad[]
                        if (columnToLoad[i]) {
                            if (cd.column().isSimple()) {
                                col = columns.get(currentColumn);
                                value = this.arrTypeSerializers.get(i).deserialize(((Cell) cd).value());
                                if(value == null)
                                    col.appendMissing();
                                else {
                                    // Convert the data before storing it to the columns
                                    // The conversion is done in 3 step which depend on arrConvertedTypes.
                                    // (1) First, this code will call the corresponding Consumer converter, except for
                                    // type CQLString and String because the conversion is handled manually below.
                                    // (2) Second, the Consumer converter will store the output at a static variable
                                    // according to its type. (3) And finally, the switch case below will retrieve the
                                    // converted value from the corresponding static variable.
                                    switch (arrConvertedTypes.get(i)) {
                                        case String:
                                            col.append(value.toString());
                                            break;
                                        case Double:
                                            this.arrConverters.get(i).accept(value);
                                            col.append(CassandraSSTableLoader.valueDouble);
                                            break;
                                        case Integer:
                                            this.arrConverters.get(i).accept(value);
                                            col.append(CassandraSSTableLoader.valueInteger);
                                            break;
                                        case CQLString:
                                            col.append(this.arrTypeSerializers.get(i)
                                                .toCQLLiteral(((Cell) cd).value()).toString());
                                            break;
                                        case Date:
                                            this.arrConverters.get(i).accept(value);
                                            col.append(CassandraSSTableLoader.valueDate);
                                            break;
                                        case Duration:
                                            this.arrConverters.get(i).accept(value);
                                            col.append(CassandraSSTableLoader.valueDuration);
                                            break;
                                        case Counter:
                                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance()
                                                .total(((Cell) cd).value()));
                                            col.append(Double.parseDouble(this.arrTypeSerializers.get(i)
                                                .toCQLLiteral(counterValue)));
                                            break;
                                        default:
                                            col.append(value);
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
                } // else tombstone marker
            }
        });
        return columns;
    }
}