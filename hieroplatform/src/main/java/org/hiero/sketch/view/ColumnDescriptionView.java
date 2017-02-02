package org.hiero.sketch.view;

import org.hiero.sketch.table.api.ContentsKind;

public class ColumnDescriptionView implements IJson {
    private final ContentsKind kind;
    private final String       name;
    private final int          sortOrder;  // 0 - invisible, >0 - ascending, <0 - descending

    public ColumnDescriptionView(ContentsKind kind, String name, int sortOrder) {
        this.kind = kind;
        this.name = name;
        this.sortOrder = sortOrder;
    }
}
