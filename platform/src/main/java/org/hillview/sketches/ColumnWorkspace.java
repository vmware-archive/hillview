package org.hillview.sketches;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISketchWorkspace;

public class ColumnWorkspace<SW> implements ISketchWorkspace {
    final IColumn column;
    final SW      childWorkspace;

    ColumnWorkspace(IColumn column, SW childWorkspace) {
        this.column = column;
        this.childWorkspace = childWorkspace;
    }
}
