package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Table;
import rx.Observable;

public class NextKSketch implements ISketch<Table, NextKList> {
    public final RowSnapshot topRow;

    public NextKSketch(RowSnapshot topRow) {
        this.topRow = topRow;
    }

    @Override
    public NextKList zero() {
        return null;
    }

    @Override
    public NextKList add(NextKList left, NextKList right) {
        return null;
    }

    @Override
    public Observable<PartialResult<NextKList>> create(Table data) {
        return null;
    }
}
