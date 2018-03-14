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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.BaseListColumn;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class ParquetReader extends TextFileLoader {
    public ParquetReader(String path) {
        super(path);
    }

    private static void appendGroup(
            IAppendableColumn[] cols, Group g, List<ColumnDescriptor> cds) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);
            if (valueCount == 0) {
                cols[field].appendMissing();
                continue;
            }
            if (valueCount > 1)
                throw new RuntimeException("Repeated values not supported");

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            if (!fieldType.isPrimitive())
                throw new RuntimeException("Non-primitive field not supported");
            switch (cds.get(field).getType()) {
                case INT64: {
                    long l = g.getLong(field, 0);
                    cols[field].append((double) l);
                    break;
                }
                case FLOAT: {
                    float f = g.getFloat(field, 0);
                    cols[field].append((double) f);
                    break;
                }
                case DOUBLE: {
                    double d = g.getDouble(field, 0);
                    cols[field].append(d);
                    break;
                }
                case INT32: {
                    int i = g.getInteger(field, 0);
                    cols[field].append(i);
                    break;
                }
                case BOOLEAN: {
                    boolean b = g.getBoolean(field, 0);
                    cols[field].append(b ? "true" : "false");
                    break;
                }
                case BINARY: {
                    Binary b = g.getBinary(field, 0);
                    String s = b.toStringUsingUTF8();
                    cols[field].append(s);
                    break;
                }
                case FIXED_LEN_BYTE_ARRAY: {
                    String s = g.getString(field, 0);
                    cols[field].append(s);
                    break;
                }
                case INT96: {
                    // We are assuming that this is a Hive/Impala timestamp
                    // from the drill ParquetReaderUtility.java file
                    final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
                    Binary val = g.getInt96(field, 0);
                    NanoTime nt = NanoTime.fromBinary(val);
                    int julianDay = nt.getJulianDay();
                    long nanosOfDay = nt.getTimeOfDayNanos();
                    long epochSeconds = (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * 24 * 60 * 60;
                    Instant inst = Instant.ofEpochSecond(epochSeconds, nanosOfDay);
                    cols[field].append(inst);
                    break;
                }
                default:
                    throw new RuntimeException("Unexpected column kind " + cds.get(field).getType());
            }
        }
    }

    IAppendableColumn[] createColumns(ParquetMetadata md) {
        List<ColumnDescriptor> cols = md.getFileMetaData().getSchema().getColumns();
        IAppendableColumn[] result = new IAppendableColumn[cols.size()];

        int index = 0;
        for (ColumnDescriptor cd: md.getFileMetaData().getSchema().getColumns()) {
            String name = cd.toString();
            ContentsKind kind;
            switch (cd.getType()) {
                case INT64:
                case FLOAT:
                case DOUBLE:
                    kind = ContentsKind.Double;
                    break;
                case INT32:
                    kind = ContentsKind.Integer;
                    break;
                case BOOLEAN:
                    kind = ContentsKind.Category;
                    break;
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    kind = ContentsKind.String;
                    break;
                case INT96:
                    kind = ContentsKind.Date;
                    break;
                default:
                    throw new RuntimeException("Unexpected column kind " + cd.getType());
            }
            ColumnDescription desc = new ColumnDescription(name, kind);
            result[index++] = BaseListColumn.create(desc);
        }
        return result;
    }

    public ITable load() {
        try {
            Configuration conf = new Configuration();
            System.setProperty("hadoop.home.dir", "/");
            conf.set("hadoop.security.authentication", "simple");
            conf.set("hadoop.security.authorization", "false");
            Path path = new Path(this.filename);
            ParquetMetadata md = ParquetFileReader.readFooter(conf, path,
                    ParquetMetadataConverter.NO_FILTER);
            MessageType schema = md.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, md);
            IAppendableColumn[] cols = this.createColumns(md);
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

            PageReadStore pages;
            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                RecordReader<Group> recordReader = columnIO.getRecordReader(
                        pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    Group g = recordReader.read();
                    appendGroup(cols, g, md.getFileMetaData().getSchema().getColumns());
                }
            }

            for (IAppendableColumn c: cols)
                c.seal();
            this.close(null);
            return new Table(cols);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
