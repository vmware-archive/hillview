package org.hiero.sketch.view;

import org.hiero.sketch.dataset.api.IJson;

@SuppressWarnings("FieldCanBeLocal")
public class TableDataView implements IJson {
    private final ColumnDescriptionView[] schema;
    private final int rowCount;
    private final int startPosition;
    private final RowView[] rows;

    public TableDataView(ColumnDescriptionView[] schema, int rowCount, int startPosition, RowView[] rows) {
        this.schema = schema;
        this.rowCount = rowCount;
        this.startPosition = startPosition;
        this.rows = rows;
    }
}
