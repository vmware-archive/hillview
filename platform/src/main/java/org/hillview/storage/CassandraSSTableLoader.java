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
import org.apache.cassandra.db.rows.ComplexColumnData;
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
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.util.stream.Stream;
import java.io.IOException;
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
public class CassandraSSTableLoader {
    @Nullable
    private Schema actualSchema;
    @Nullable
    private final String ssTablePath;
    @Nullable
    IAppendableColumn[] columns;
    @Nullable
    private String currentToken;

    private int currentColumn;
    private int currentRow;
    private boolean allowFewerColumns;

    static
    {
        /** Initializing DatabaseDescriptor that contains Cassandra's general configuration */
        DatabaseDescriptor.clientInitialization(false);
    }
    
    public CassandraSSTableLoader(String ssTablePath) {
        this.ssTablePath = ssTablePath;
    }

    public CFMetaData getSSTableMetadata(Descriptor desc) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    private <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    /** Converting Cassandra's data type to Hillview data type */
    private static ColumnDescription getDescription(ColumnDefinition colDef)
            throws Exception {
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
            case "blob":
            case "empty":
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

    private void append(String[] data) {
        try {
            assert this.columns != null;
            int columnCount = this.columns.length;
            this.currentColumn = 0;
            if (data.length > columnCount)
                throw new RuntimeException("Too many columns " + data.length + " vs " + columnCount);
            for (this.currentColumn = 0; this.currentColumn < data.length; this.currentColumn++) {
                this.currentToken = data[this.currentColumn];
                this.columns[this.currentColumn].parseAndAppendString(this.currentToken);
            }
            if (data.length < columnCount) {
                if (!this.allowFewerColumns)
                throw new RuntimeException("Too few columns " + data.length + " vs " + columnCount);
                else {
                    this.currentToken = "";
                    for (int i = data.length; i < columnCount; i++)
                        this.columns[i].parseAndAppendString(this.currentToken);
                }
            }
            this.currentRow++;
            this.currentToken = null;
        } catch (Exception ex) {
            HillviewLogger.instance.error("Error getting row #"+this.currentRow+" from sstable " + this.ssTablePath);
            throw new RuntimeException(ex);
        }
    }

    private void serializePartition( UnfilteredRowIterator partition) {
        try {
            int i;
            String value;
            String[] line;
            Row row;
            Cell cell;
            AbstractType<?> cellType;
            Unfiltered unfiltered;
            while (partition.hasNext())
            {
                unfiltered = partition.next();
                if (unfiltered instanceof Row)
                {
                    row = (Row) unfiltered;
                    line = new String[row.columnCount()];
                    i = 0;
                    /** Extracting the value for each column from a single row  */
                    for (ColumnData cd : row) {
                        if (cd.column().isSimple()){
                            cell = (Cell) cd;
                            cellType = cell.column().cellValueType();
                            value = cellType.getSerializer().deserialize(cell.value()).toString();
                            line[i] = value; 
                        }else{
                            /** Hillview cooncatenates string representation of the complex data */
                            ComplexColumnData complexData = (ComplexColumnData) cd;
                            value = null;
                            for (Cell c : complexData) {
                                cellType = c.column().cellValueType();
                                value += cellType.getSerializer().deserialize(c.value()).toString() + "; ";
                            }
                        }
                        i++;
                    }
                    this.append(line);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ITable load() {
        try {
            Descriptor desc = Descriptor.fromFilename(this.ssTablePath);
            CFMetaData metadata = this.getSSTableMetadata(desc);
            SSTableReader ssTableReader = SSTableReader.openNoValidation(desc, metadata);
            ISSTableScanner currentScanner = ssTableReader.getScanner();
            Stream<UnfilteredRowIterator> partitions = this.iterToStream(currentScanner).filter(i -> true);

            // Get the schema of the current SSTable
            this.actualSchema = this.getSchema(metadata);
            // Print the schema using: printSchema(metadata);
            
            assert this.actualSchema != null;
            this.columns = this.actualSchema.createAppendableColumns();

            // This is iterating each item inside the table
            partitions.forEach(partition -> {
                this.serializePartition(partition);
            });

            IColumn[] sealed = new IColumn[this.columns.length];
            IMembershipSet ms = null;
            IAppendableColumn c;
            IColumn s;
            for (int ci = 0; ci < this.columns.length; ci++) {
                c = this.columns[ci];
                s = c.seal();
                if (ms == null)
                    ms = new FullMembershipSet(s.sizeInRows());
                sealed[ci] = s;
                assert sealed[ci] != null;
            }

            return new Table(sealed, this.ssTablePath, null);
        } catch (Exception e) {
            HillviewLogger.instance.error("Could not retrieve sstable data from " + this.ssTablePath);
            throw new RuntimeException(e);
        } 
    }

    public static ITable ssTableTest(){
        try{
            String ssTablePath = "/Users/daniar/Documents/Github/hillview/data/sstable/md-2-big-Data.db";
            CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(ssTablePath);
            HillviewLogger.instance.info("Loading SSTable", "{0}", ssTablePath);

            ITable table = ssTableLoader.load();
            // System.out.println("Loaded table: " + table.toString());
            return table;
        } catch (Exception e) {
            e.printStackTrace(); 
            throw new RuntimeException(e);
        }
    }

}