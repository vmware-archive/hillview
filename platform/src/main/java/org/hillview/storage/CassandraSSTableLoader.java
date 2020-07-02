/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.util.stream.Stream;
import java.util.*;
import java.util.stream.StreamSupport;

import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Knows how to read a local Cassandra's SSTable file.
 */
public class CassandraSSTableLoader extends TextFileLoader{
    @Nullable
    private final Schema actualSchema;
    private final String ssTablePath;
    @Nullable
    IAppendableColumn[] columns;

    public boolean lazyLoading;
    private int currentColumn;
    private long rowCount;
    private final CFMetaData metadata;
    private final SSTableReader ssTableReader;

    static{
        /* Initializing DatabaseDescriptor that contains Cassandra's general configuration */
        DatabaseDescriptor.clientInitialization(false);
    }
    
    public CassandraSSTableLoader(String ssTablePath, boolean lazyLoading) {
        super(ssTablePath);
        this.ssTablePath = ssTablePath;
        this.lazyLoading = lazyLoading;

        try{
            Descriptor descriptor = Descriptor.fromFilename(this.ssTablePath);
            this.metadata = this.getSSTableMetadata(descriptor);
            this.ssTableReader = SSTableReader.openNoValidation(descriptor, this.metadata);
            /* Get the schema of the current SSTable */
            this.actualSchema = this.getSchema(this.metadata);
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing Metadata and SStable partitions based on: " + this.ssTablePath);
            throw new RuntimeException(e);
        }
    }

    public CFMetaData getSSTableMetadata(Descriptor desc) throws Exception{
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
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
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
                kind = ContentsKind.String;
                break;
            case "counter":
            case "int":
            case "smallint":
            case "tinyint":
            case "varint":
                kind = ContentsKind.Integer;
                break;
            case "bigint":
            case "decimal":
            case "double":
            case "float":
                kind = ContentsKind.Double;
                break;
            case "time":
            case "timestamp":
            case "date":
                kind = ContentsKind.Date;
                break;
            case "duration":
                kind = ContentsKind.Duration;
                break;
            case "empty":
                kind = ContentsKind.None;
                break;
            case "blob":
            default:
                throw new RuntimeException("Unhandled column type " + colType);
        }
        return new ColumnDescription(name, kind);
    }

    public void printSchema(CFMetaData metadata) {
        System.out.println("Column size : " + metadata.partitionColumns().size());
        PartitionColumns columnDefinitions = metadata.partitionColumns();
        for (ColumnDefinition colDef : columnDefinitions) {
            System.out.println(colDef.toString() + " " + colDef.type.asCQL3Type());
        }
    }

    public Schema getSchema(CFMetaData metadata) {
        try {
            Schema result = new Schema();
            PartitionColumns columnDefinitions = metadata.partitionColumns();
            for (ColumnDefinition colDef : columnDefinitions) {
                ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                result.append(cd);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class SSTableColumnLoader implements IColumnLoader {
        private int currentColumn;
        private final SSTableReader ssTableReader;
        private final Schema actualSchema;
        private int sizeToLoad;

        SSTableColumnLoader(SSTableReader ssTableReader, Schema actualSchema){
            this.ssTableReader = ssTableReader;
            this.actualSchema = actualSchema;
        }

        /** Marking the corresponding column index that are included in List<String> names */
        private boolean[] getColumnMarker(List<String> names){
            this.sizeToLoad = 0;
            List<String> colNames = this.actualSchema.getColumnNames();
            boolean[] result = new boolean[colNames.size()];
            int i = 0;
            for (String cn : colNames) {
                if(names.contains(cn)){
                    result[i] = true;
                    /* Incrementing the number of column that will be loaded  */
                    this.sizeToLoad++;
                }else{
                    result[i] = false;
                }
                i++;
            }
            return result;
        }
        
        @Override
        public List<IColumn> loadColumns(List<String> names) {
            IAppendableColumn[] columns = this.actualSchema.createAppendableColumns();
            boolean[] columnToLoad = getColumnMarker(names);
            ISSTableScanner currentScanner = this.ssTableReader.getScanner();
            Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner, Spliterator.IMMUTABLE);
            Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);
            
            return CassandraSSTableLoader.this.loadColumns(columns, columnToLoad, partitions, this.sizeToLoad );
        }
    }

    public int getNumRows() {
        ISSTableScanner currentScanner = this.ssTableReader.getScanner();
        Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner, Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);
        /* Iterating each item inside the table */
        partitions.forEach(partition -> {
            Unfiltered unfiltered;
            /* Serializing each partition */
            while (partition.hasNext()){
                unfiltered = partition.next();
                /* Hillview ignores Tombstone Markers, just count the Row */
                if (unfiltered instanceof Row){
                    this.rowCount++;
                }
            }
        });
        return Converters.toInt(rowCount);
    }

    public ITable load(){
        try {
            this.columns = this.actualSchema.createAppendableColumns();
            if (this.lazyLoading) {
                int size = this.getNumRows();
                PartitionColumns columnDefinitions = metadata.partitionColumns();
                List<ColumnDescription> cds = new ArrayList<ColumnDescription>(columnDefinitions.size());
                for (ColumnDefinition colDef : columnDefinitions) {
                    ColumnDescription cd = CassandraSSTableLoader.getDescription(colDef);
                    cds.add(cd);
                }
                assert this.actualSchema != null;
                IColumnLoader loader = new CassandraSSTableLoader.SSTableColumnLoader(this.ssTableReader, this.actualSchema);
                return Table.createLazyTable(cds, size, this.filename, loader);
            } else {
                int sizeToLoad = this.columns.length;
                /** columns loader will load all column, so all item of columnToLoad need to be TRUE */
                boolean[] columnToLoad = new boolean[sizeToLoad];
                Arrays.fill(columnToLoad, Boolean.TRUE);

                ISSTableScanner currentScanner = this.ssTableReader.getScanner();
                Spliterator<UnfilteredRowIterator> splititer = Spliterators.spliteratorUnknownSize(currentScanner, Spliterator.IMMUTABLE);
                Stream<UnfilteredRowIterator> partitions = StreamSupport.stream(splititer, false);

                List<IColumn> arrColumns = loadColumns(this.columns ,columnToLoad, partitions, sizeToLoad);
                return new Table(arrColumns, this.ssTablePath, null);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private List<IColumn> loadColumns( IAppendableColumn[] columns, boolean[] columnToLoad, Stream<UnfilteredRowIterator> partitions, int sizeToLoad){
        /* Iterating each item inside the table */
        partitions.forEach(partition -> {
            int i;
            Object value;
            Object[] line;
            Row row;
            Cell cell;
            AbstractType<?> cellType;
            Unfiltered unfiltered;

            /* Serializing each partition */
            while (partition.hasNext()){
                unfiltered = partition.next();
                if (unfiltered instanceof Row){
                    row = (Row) unfiltered;
                    line = new Object[row.columnCount()];
                    i = 0;
                    /* Extracting the value for each column from a single row  */
                    for (ColumnData cd : row) {
                        /* Filter the column to only load the one marked true by columnToLoad[] */
                        if (columnToLoad[i]){
                            if (cd.column().isSimple()){
                                cell = (Cell) cd;
                                cellType = cell.column().cellValueType();
                                value = cellType.getSerializer().deserialize(cell.value());
                            }else{
                                /* Hillview won't process the complex data */
                                throw new RuntimeException("[Hillview can't convert complex data]");
                            }
                            line[i] = value; 
                        }
                        i++;
                    }
                    /* Append each line to Hillview column */
                    for (this.currentColumn = 0; this.currentColumn < line.length; this.currentColumn++) {
                        columns[this.currentColumn].append(line[this.currentColumn]);
                    }
                } else if (unfiltered instanceof RangeTombstoneMarker) {
                    /* Hillview ignores Tombstone Markers */
                }
            }
        });

        List<IColumn> arrColumns = new ArrayList<IColumn>(sizeToLoad);
        IMembershipSet membershipSet = null;
        IAppendableColumn appendableColumn;
        IColumn column;
        for (int idx = 0; idx < columns.length; idx++) {
            appendableColumn = columns[idx];
            column = appendableColumn.seal();
            if (membershipSet == null)
                membershipSet = new FullMembershipSet(column.sizeInRows());
            if(columnToLoad[idx])
                arrColumns.add(column);
        }
        return arrColumns;
    }
}