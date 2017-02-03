package org.hiero.sketch.view;

import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.api.ContentsKind;

@SuppressWarnings("WeakerAccess")
public class ColumnDescriptionView implements IJson {
    public final ContentsKind kind;
    public final String       name;
    public final boolean      allowMissing;
    public final int          sortOrder;  // 0 - invisible, >0 - ascending, <0 - descending

    public ColumnDescriptionView(ContentsKind kind, String name,
                                 boolean allowMissing, int sortOrder) {
        this.kind = kind;
        this.name = name;
        this.allowMissing = allowMissing;
        this.sortOrder = sortOrder;
    }

    public ColumnSortOrientation toOrientation() {
        if (this.sortOrder == 0)
            return null;
        ColumnDescription cd = new ColumnDescription(this.name, this.kind, this.allowMissing);
        return new ColumnSortOrientation(cd, this.sortOrder > 0);
    }
}
