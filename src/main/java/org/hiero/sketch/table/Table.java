package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table {
    private ISchema schema;
    private IColumn[] columns;
    private IMembershipSet members;

    public Table(ISchema schema, IColumn[] columns, IMembershipSet members) {
        this.schema = schema;
        this.columns = columns;
        this.members = members;
    }

    /**
     * Generates a table that contains only the columns     refereed to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     */
    public Table compress(ISubSchema subSchema) {
        ISchema newSchema = this.schema.project(subSchema);
        int width = newSchema.getColumnCount();
        IColumn[] compressedCols = new IColumn[width];
        for (int i = 0; i < width; i++) {
            String colName = newSchema.getDescription(i).name;
            int j = this.schema.getColumnIndex(colName);
            compressedCols[i] = this.columns[j].compress(this.members);
        }
        IMembershipSet full = new FullMembership(this.members.getSize());
        Table result = new Table(newSchema, compressedCols, full);
        return result;
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set with consecutive numbering.
     */
    public Table compress() {
        ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema);
    }

    public void printStats(){
        System.out.printf("No of columns: %d, No of rows: %d%n",
                this.schema.getColumnCount(), this.members.getSize());
    }
}

