package org.hiero.sketch.view;

import org.hiero.sketch.table.api.ContentsKind;
import scala.reflect.internal.util.TableDef;

public class ColumnDescriptionView implements IJson {
    public final ContentsKind kind;
    public final String       name;
    public final int          sortOrder;  // 0 - invisible, >0 - ascending, <0 - descending

    public ColumnDescriptionView(ContentsKind kind, String name, int sortOrder) {
        this.kind = kind;
        this.name = name;
        this.sortOrder = sortOrder;
    }
}
