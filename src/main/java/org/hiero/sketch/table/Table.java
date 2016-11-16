package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table {
    private final ISchema schema;
    private final IColumn[] columns;
    private final IMembershipSet members;

    public Table(final ISchema schema, final IColumn[] columns, final IMembershipSet members) {
        this.schema = schema;
        this.columns = columns;
        this.members = members;
    }

    /**
     * Generates a table that contains only the columns     refereed to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     */
    public Table compress(final ISubSchema subSchema) {
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

    public void printStats(){
        System.out.printf("No of columns: %d, No of rows: %d%n",
                this.schema.getColumnCount(), this.members.getSize());
    }
}
