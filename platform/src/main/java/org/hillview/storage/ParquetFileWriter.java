package org.hillview.storage;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;

public class ParquetFileWriter implements ITableWriter {
    private final String filepath;

    public ParquetFileWriter(String filepath) {
        this.filepath = filepath;
    }

    private static MessageType getMessageType(Schema schema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        for (ColumnDescription description : schema.getColumnDescriptions()) {
            switch (description.kind) {
                case Date:
                    builder.optional(PrimitiveType.PrimitiveTypeName.INT64)
                            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(description.name);
                    break;
                case Double:
                    builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(description.name);
                    break;
                case Integer:
                    builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(description.name);
                    break;
                case LocalDate:
                    builder.optional(PrimitiveType.PrimitiveTypeName.INT64)
                            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(description.name);
                    break;
                /** Although parquet has the JSON logical type [1], avro doesn't [2]
                 * If we annotate with the json type here, the annotation will be lost after conversion to avro schema
                 * Therefore here we treat JSON the same way as string
                 * references:
                 * [1]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json
                 * [2]: https://avro.apache.org/docs/current/spec.html#Logical+Types
                 **/
                case Json:
                case String:
                    builder.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named(description.name);
                    break;
                case Time:
                    builder.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(description.name);
                    break;
                case Duration:
                case Interval:
                case None:
                default:
                    throw new RuntimeException("Unsupported type: " + description.kind);
            }
        }

        // Not sure if this name should be made a variable
        return builder.named("schema");
    }

    @Override
    public void writeTable(ITable table) {
        MessageType messageType = getMessageType(table.getSchema());
        org.apache.avro.Schema avroSchema = new AvroSchemaConverter().convert(messageType);
        List<IColumn> cols = table.getLoadedColumns(table.getSchema().getColumnNames());

        try {
            ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(filepath))
                    .withSchema(avroSchema)
                    .build();

            IRowIterator iterator = table.getMembershipSet().getIterator();
            int nextRow = iterator.getNextRow();
            while (nextRow >= 0) {
                GenericData.Record record = new GenericData.Record(avroSchema);
                for (int i = 0; i < cols.size(); i++) {
                    IColumn col = cols.get(i);

                    if (col.isMissing(nextRow)) {
                        record.put(col.getName(), null);
                        continue;
                    }

                    switch (col.getKind()) {
                        case Date:
                            record.put(col.getName(), Converters.toDate(col.getDouble(nextRow)).toEpochMilli());
                            break;
                        case Double:
                        case Duration:
                            record.put(col.getName(), col.getDouble(nextRow));
                            break;
                        case Integer:
                            record.put(col.getName(), col.getInt(nextRow));
                            break;
                        case LocalDate:
                            record.put(col.getName(),
                                    Converters.toLocalDate(col.getDouble(nextRow)).atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli());
                            break;
                        case Json:
                        case String:
                            record.put(col.getName(), col.getString(nextRow));
                            break;
                        case Time:
                            record.put(col.getName(),
                                    Converters.toTime(col.getDouble(nextRow)).toNanoOfDay() / Converters.NANOS_TO_MILLIS);
                            break;
                        case Interval:
                        case None:
                        default:
                            throw new RuntimeException("Unsupported type: " + col.getKind());
                    }
                }

                writer.write(record);
                nextRow = iterator.getNextRow();
            }
            writer.close();
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }
}
