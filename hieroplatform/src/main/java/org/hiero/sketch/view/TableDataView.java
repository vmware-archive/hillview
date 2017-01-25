package org.hiero.sketch.view;

public class TableDataView implements IJson {
    public final ColumnDescriptionView[] schema;
    public final int rowCount;
    public final int startPosition;
    public final RowView[] rows;

    public TableDataView(ColumnDescriptionView[] schema, int rowCount, int startPosition, RowView[] rows) {
        this.schema = schema;
        this.rowCount = rowCount;
        this.startPosition = startPosition;
        this.rows = rows;
    }
}
