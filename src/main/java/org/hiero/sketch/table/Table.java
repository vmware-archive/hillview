package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.*;
import scala.reflect.internal.util.TableDef;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table {
    @NonNull
    private final ISchema schema;
    @NonNull
    private final IColumn[] columns;
    @NonNull
    private final IMembershipSet members;

    public Table(@NonNull final ISchema schema,
                 @NonNull final IColumn[] columns,
                 @NonNull final IMembershipSet members) {
        this.schema = schema;
        this.columns = columns;
        this.members = members;
        for (IColumn c : columns) {
            int ci = schema.getColumnIndex(c.getName());
            ColumnDescription cd = schema.getDescription(ci);
            if (!c.getDescription().equals(cd))
                throw new IllegalArgumentException("Schema mismatch " + cd.toString() +
                        " vs. " + c.getDescription().toString());
        }
    }

    public Table(@NonNull final IColumn[] columns, @NonNull final IMembershipSet members) {
        Schema s = new Schema();
        for (IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.columns = columns;
        this.members = members;
    }

    /**
     * Generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    public Table compress(@NonNull final ISubSchema subSchema) {
        final ISchema newSchema = this.schema.project(subSchema);
        final int width = newSchema.getColumnCount();
        final IColumn[] compressedCols = new IColumn[width];
        for (int i = 0; i < width; i++) {
            final String colName = newSchema.getDescription(i).name;
            final int j = this.schema.getColumnIndex(colName);
            compressedCols[i] = this.columns[j].compress(this.members);
        }
        final IMembershipSet full = new FullMembership(this.members.getSize());
        return new Table(newSchema, compressedCols, full);
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set with consecutive numbering.
     */
    public Table compress() {
        final ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema);
    }

    public String toString(){
        return("Table, " + this.schema.getColumnCount() + " columns, "
                + this.members.getSize() + " rows");
    }
}
