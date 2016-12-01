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
     * Generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    public Table compress(final ISubSchema subSchema, final IMembershipSet memberSet) {
        final ISchema newSchema = this.schema.project(subSchema);
        final int width = newSchema.getColumnCount();
        final IColumn[] compressedCols = new IColumn[width];
        for (int i = 0; i < width; i++) {
            final String colName = newSchema.getDescription(i).name;
            final int j = this.schema.getColumnIndex(colName);
            compressedCols[i] = this.columns[j].compress(memberSet);
             }
        final IMembershipSet full = new FullMembership(memberSet.getSize());
        return new Table(newSchema, compressedCols, full);
    }

    /**
     * @param memberSet Set of rows to include in the compressed table.
     * @return A compressed table containing only the rows contained in MemberSet.
     */
    public Table compress(final IMembershipSet memberSet) {
        final ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema, memberSet);
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set members with consecutive numbering.
     */
    public Table compress() {
        final ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema, this.members);
    }

    /**
     * Creates a sample Table for an input Table,
     * @param numSamples The number of samples
     * @return A table of samples.
     */
    public Table sampleTable(final int numSamples) {
        final int dataSize = this.getNumOfRows();
        final IMembershipSet sampleSet = this.members.sample(numSamples);
        return this.compress(sampleSet);
    }

    public String toString() {
        return("Table, " + this.schema.getColumnCount() + " columns, "
                + this.members.getSize() + " rows");
    }

    public void printTable() {
        System.out.printf("%s %n", this.toString());
        IRowIterator rowIt = this.members.getIterator();
        int nextRow = rowIt.getNextRow();
        while(nextRow != -1) {
            for (IColumn nextCol: columns){
                System.out.printf("%s, ", nextCol.asString(nextRow));
            }
            System.out.printf("%n");
            nextRow = rowIt.getNextRow();
        }
    }

    public int getColumnIndex(final String colName) {
        return this.schema.getColumnIndex(colName);
    }

    public IColumn getColumn(final String colName) {
        return this.columns[this.schema.getColumnIndex(colName)];
    }

    public IColumn getColumn(final int index) {
        return this.columns[index];
    }

    public int getNumOfRows() {
        return this.members.getSize();
    }
}
