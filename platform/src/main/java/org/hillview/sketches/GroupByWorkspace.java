package org.hillview.sketches;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.utils.JsonList;

public class GroupByWorkspace<SW> implements ISketchWorkspace {
    final IColumn column;
    final JsonList<SW> bucketWorkspace;   // one per bucket
    final SW missingWorkspace;

    GroupByWorkspace(IColumn column, JsonList<SW> bucketWorkspace, SW missingWorkspace) {
        this.column = column;
        this.bucketWorkspace = bucketWorkspace;
        this.missingWorkspace = missingWorkspace;
    }
}
