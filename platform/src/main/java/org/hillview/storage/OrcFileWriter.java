/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.*;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Writes Hillview ITable objects into Apache Orc files
 * https://orc.apache.org
 * https://orc.apache.org/docs/core-java.html
 */
public class OrcFileWriter implements ITableWriter {
    private final String path;

    public OrcFileWriter(String path) {
        this.path = path;
    }

    private static TypeDescription getSchema(Schema schema) {
        TypeDescription result = TypeDescription.createStruct();
        for (String col : schema.getColumnNames()) {
            ColumnDescription cd = schema.getDescription(col);
            TypeDescription current;
            switch (cd.kind) {
                case None:
                default:
                    throw new RuntimeException("Unexpected data type " + cd.kind);
                case Category:
                case String:
                case Json:
                    current = TypeDescription.createString();
                    break;
                case Date:
                    current = TypeDescription.createTimestamp();
                    break;
                case Integer:
                    current = TypeDescription.createInt();
                    break;
                case Duration:
                case Double:
                    current = TypeDescription.createDouble();
                    break;
            }
            result.addField(col, current);
        }
        return result;
    }

    @Override
    public void writeTable(ITable table) {
        try {
            Configuration conf = new Configuration();
            TypeDescription schema = getSchema(table.getSchema());
            Writer writer = OrcFile.createWriter(new Path(this.path),
                    OrcFile.writerOptions(conf).setSchema(schema));
            VectorizedRowBatch batch = schema.createRowBatch();

            IRowIterator rowIter = table.getMembershipSet().getIterator();
            int nextRow = rowIter.getNextRow();
            int batchSize = batch.getMaxSize();
            List<ColumnAndConverterDescription> allColumns = table.getSchema().getColumnAndConverterDescriptions();
            List<ColumnAndConverter> cols = table.getLoadedColumns(allColumns);

            while (nextRow >= 0) {
                int outRowNo = batch.size;
                for (int i = 0; i < batch.cols.length; i++) {
                    ColumnVector cv = batch.cols[i];
                    IColumn col = cols.get(i).column;
                    if (col.isMissing(nextRow)) {
                        cv.noNulls = false;
                        cv.isNull[outRowNo] = true;
                        continue;
                    }

                    switch (col.getKind()) {
                        case None:
                            break;
                        case Category:
                        case String:
                        case Json:
                            String s = col.getString(nextRow);
                            assert s != null;
                            ((BytesColumnVector)cv).setVal(outRowNo, s.getBytes());
                            break;
                        case Date:
                            Instant inst = col.getDate(nextRow);
                            assert inst != null;
                            TimestampColumnVector tscv = (TimestampColumnVector)cv;
                            tscv.time[outRowNo] = inst.toEpochMilli();
                            tscv.nanos[outRowNo] = inst.getNano();
                            break;
                        case Integer:
                            int iv = col.getInt(nextRow);
                            ((LongColumnVector)cv).vector[outRowNo] = iv;
                            break;
                        case Double:
                        case Duration:
                            // TODO: durations are doubles, not clear what else we can do
                            double d = col.getDouble(nextRow);
                            ((DoubleColumnVector)cv).vector[outRowNo] = d;
                            break;
                    }
                }

                nextRow = rowIter.getNextRow();
                batch.size++;
                if (batch.size == batchSize) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
