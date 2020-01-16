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
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.Linq;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * A Loader for Apache ORC file formats
 * https://orc.apache.org
 */
public class OrcFileLoader extends TextFileLoader {
    private final boolean lazy;
    private final Configuration conf = new Configuration();
    /**
     * Path of the Hillview Schema if specified.
     */
    @Nullable
    private final String schemaPath;
    /**
     * Orc schema of the full file.
     */
    @Nullable
    private TypeDescription schema = null;
    /**
     * The user may specify a schema file separately.
     * This is useful because e.g., converting strings to categories.
     */
    @Nullable
    private Schema hillviewSchema = null;

    public OrcFileLoader(String path, @Nullable String schemaPath, boolean lazy) {
        super(path);
        this.lazy = lazy;
        this.schemaPath = schemaPath;
    }

    private boolean[] project(List<String> columns) {
        assert this.schema != null;
        List<String> fields = this.schema.getFieldNames();
        boolean[] toInclude = new boolean[fields.size() + 1];
        // 1 for the Category.STRUCT TypeDescription, which has index 0.
        // This does not seem to be documented in the Orc file format...
        toInclude[0] = false;  // the struct type itself

        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            toInclude[i + 1] = columns.contains(field);
        }
        return toInclude;
    }

    class OrcColumnLoader implements IColumnLoader {
        @Override
        public List<IColumn> loadColumns(List<String> names) {
            try {
                boolean[] toRead = OrcFileLoader.this.project(names);
                Reader.Options options = new Reader.Options();
                options = options.include(toRead);
                Reader reader = OrcFile.createReader(new Path(filename),
                        OrcFile.readerOptions(OrcFileLoader.this.conf));
                List<IAppendableColumn> result = readColumns(
                        reader, options, OrcFileLoader.this.hillviewSchema);
                return Linq.map(result, e -> e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * For simple ORC TypeDescriptions it returns the corresponding ColumnKind.
     */
    private static ContentsKind getKind(TypeDescription desc) {
        switch (desc.getCategory()) {
            case BOOLEAN:
            case STRING:
                return ContentsKind.String;
            case BYTE:
            case SHORT:
            case INT:
                return ContentsKind.Integer;
            case LONG:
            case FLOAT:
            case DOUBLE:
                return ContentsKind.Double;
            case DATE:
            case TIMESTAMP:
                return ContentsKind.Date;
            case BINARY:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
            case LIST:
            case MAP:
            case STRUCT:
            case UNION:
            default:
                throw new RuntimeException("Compltex ORC column kind not supported in Hillview "
                        + desc.getCategory());
        }
    }

    private static List<ColumnDescription> getDescriptions(TypeDescription complex) {
        if (complex.getCategory() != TypeDescription.Category.STRUCT)
            throw new RuntimeException("Expected a Struct TypeDescription, got " + complex);
        List<String> fields = complex.getFieldNames();
        List<TypeDescription> types = complex.getChildren();
        assert fields.size() == types.size();
        List<ColumnDescription> result = new ArrayList<ColumnDescription>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            ContentsKind kind = getKind(types.get(i));
            ColumnDescription desc = new ColumnDescription(fields.get(i), kind);
            result.add(desc);
        }
        return result;
    }

    /**
     * Append the data in the vec column to the data in the to column.
     * @param to      Data in Hillview format to append to.
     * @param vec     Data in Orc format to append.
     * @param simple  Description of the vec column type; should be a simple type.
     * @param count   Number of rows to append.
     */
    private static void appendColumn(IAppendableColumn to, ColumnVector vec,
                                     TypeDescription.Category simple, int count) {
        // See for example
        // https://github.com/apache/orc/blob/master/java/mapreduce/src/java/org/apache/orc/mapred/OrcMapredRecordReader.java
        for (int iRow=0; iRow < count; iRow++) {
            int row = iRow;
            if (vec.isRepeating)
                row = 0;
            if (!vec.noNulls && vec.isNull[row]) {
                to.appendMissing();
                continue;
            }
            switch (simple) {
                case BOOLEAN:
                    break;
                case BYTE:
                case SHORT:
                case INT:
                case LONG: {
                    LongColumnVector lcv = (LongColumnVector) vec;
                    long l = lcv.vector[row];
                    switch (to.getKind()) {
                        case None:
                        case Date:
                        case Duration:
                            throw new RuntimeException("Cannot convert long to " + to.getKind());
                        case Json:
                        case String:
                            to.append(Long.toString(l));
                            break;
                        case Integer:
                            to.append((int) l);
                            break;
                        case Double:
                            to.append((double) l);
                            break;
                    }
                    break;
                }
                case FLOAT:
                case DOUBLE: {
                    DoubleColumnVector dcv = (DoubleColumnVector) vec;
                    double d = dcv.vector[row];
                    switch (to.getKind()) {
                        case None:
                        case Date:
                        case Duration:
                            throw new RuntimeException("Cannot convert double to " + to.getKind());
                        case Json:
                        case String:
                            to.append(Double.toString(d));
                            break;
                        case Integer:
                            to.append(Utilities.toInt(d));
                            break;
                        case Double:
                            to.append(d);
                            break;
                    }
                    break;
                }
                case STRING: {
                    BytesColumnVector bcv = (BytesColumnVector) vec;
                    String str = new String(bcv.vector[row], bcv.start[row], bcv.length[row]);
                    switch (to.getKind()) {
                        case None:
                        case Date:
                        case Duration:
                            throw new RuntimeException("Cannot convert string to " + to.getKind());
                        case Json:
                        case String:
                            to.append(str);
                            break;
                        case Integer:
                            if (str.trim().isEmpty())
                                to.appendMissing();
                            else
                                to.append(Integer.parseInt(str));
                            break;
                        case Double:
                            if (str.trim().isEmpty())
                                to.appendMissing();
                            else
                                to.append(Double.parseDouble(str));
                            break;
                    }
                    break;
                }
                case DATE: {
                    LongColumnVector lcv = (LongColumnVector) vec;
                    long l = lcv.vector[row];
                    // see https://orc.apache.org/docs/encodings.html
                    // Dates do not have hours/minutes, just a number of days
                    Instant instant = Instant.ofEpochSecond(0);
                    Instant value = instant.plus(l, ChronoUnit.DAYS);
                    switch (to.getKind()) {
                        case None:
                        case Integer:
                        case Duration:
                            throw new RuntimeException("Cannot convert ORC date to "
                                    + to.getKind());
                        case Json:
                        case String:
                            to.append(Converters.toString(value));
                            break;
                        case Double:
                            to.append(Converters.toDouble(value));
                            break;
                        case Date:
                            to.append(value);
                            break;
                    }
                    break;
                }
                case TIMESTAMP: {
                    TimestampColumnVector tcv = (TimestampColumnVector) vec;
                    long time = tcv.time[row];
                    int nanos = tcv.nanos[row];
                    Instant instant = Instant.ofEpochMilli(time);
                    instant = instant.plusNanos(nanos);
                    switch (to.getKind()) {
                        case None:
                        case Integer:
                        case Duration:
                            throw new RuntimeException("Cannot convert ORC timestamp to "
                                    + to.getKind());
                        case Json:
                        case String:
                            to.append(Converters.toString(instant));
                            break;
                        case Double:
                            to.append(Converters.toDouble(instant));
                            break;
                        case Date:
                            to.append(instant);
                            break;
                    }
                    break;
                }
                case BINARY:
                case DECIMAL:
                case VARCHAR:
                case CHAR:
                case LIST:
                case MAP:
                case STRUCT:
                case UNION:
                    throw new RuntimeException("Unsupported ORC column type " + simple);
            }
        }
    }

    private static List<IAppendableColumn> readColumns(
            Reader reader, Reader.Options options, @Nullable Schema hillviewSchema)
            throws IOException {
        RecordReader rows = reader.rows(options);
        TypeDescription schema = reader.getSchema();
        List<ColumnDescription> desc = getDescriptions(schema);
        List<ColumnDescription> hillviewDesc = null;
        if (hillviewSchema != null)
            hillviewDesc = hillviewSchema.getColumnDescriptions();
        boolean[] include = options.getInclude();
        List<IAppendableColumn> toCreate = new ArrayList<IAppendableColumn>();

        for (int i = 0; i < desc.size(); i++) {
            if (include == null || include[i + 1]) {
                // include has one extra element at the start
                ColumnDescription col;
                if (hillviewDesc != null)
                    col = hillviewDesc.get(i);
                else
                    col = desc.get(i);
                toCreate.add(BaseListColumn.create(col));
            }
        }

        VectorizedRowBatch batch = schema.createRowBatch();
        while (rows.nextBatch(batch)) {
            int index = 0;
            for (int i=0; i < batch.cols.length; i++) {
                if (include != null && !include[i + 1])
                    continue;
                appendColumn(toCreate.get(index), batch.cols[i],
                        schema.getChildren().get(i).getCategory(), batch.size);
                index++;
            }
        }
        rows.close();
        return toCreate;
    }

    @Override
    public ITable load() {
        try {
            if (this.schemaPath != null)
                this.hillviewSchema = Schema.readFromJsonFile(Paths.get(this.schemaPath));
            Reader reader = OrcFile.createReader(new Path(this.filename),
                    OrcFile.readerOptions(conf));
            this.schema = reader.getSchema();
            assert this.schema != null;
            Table result;

            if (this.lazy) {
                IColumnLoader lazyLoader = new OrcColumnLoader();
                List<ColumnDescription> desc = getDescriptions(this.schema);
                if (hillviewSchema != null) {
                    List<ColumnDescription> imposed = hillviewSchema.getColumnDescriptions();
                    if (imposed.size() != desc.size())
                        throw new RuntimeException("Schema in JSON file does not match Orc schema");
                    desc = imposed;
                }
                long rowCount = reader.getNumberOfRows();
                result = Table.createLazyTable(desc, Utilities.toInt(rowCount), this.filename, lazyLoader);
            } else {
                Reader.Options options = new Reader.Options();
                List<IAppendableColumn> cols = readColumns(reader, options, this.hillviewSchema);
                this.close(null);
                result = new Table(cols, this.filename, null);
            }
            return result;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
