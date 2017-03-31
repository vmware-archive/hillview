package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IRow;
import org.hiero.sketch.table.api.ITable;

import javax.annotation.Nullable;
import java.util.HashMap;

public class ExactFreqSketch implements ISketch<ITable, HashMap<IRow, Integer>> {
    @Nullable
    @Override
    public HashMap<IRow, Integer> zero() {
        return null;
    }

    @Nullable
    @Override
    public HashMap<IRow, Integer> add(@Nullable HashMap<IRow, Integer> left, @Nullable HashMap<IRow, Integer> right) {
        return null;
    }

    @Override
    public HashMap<IRow, Integer> create(ITable data) {
        return null;
    }
}
