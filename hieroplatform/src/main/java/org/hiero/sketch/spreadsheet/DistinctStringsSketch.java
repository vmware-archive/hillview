package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.ITable;

public class DistinctStringsSketch implements ISketch<ITable, DistinctStrings> {
    private final int maxSize;
    private final String colName;

    public DistinctStringsSketch(int maxSize, String colName) {
        this.maxSize = maxSize;
        this.colName = colName;
    }

    @Override
    public DistinctStrings zero() { return new DistinctStrings(this.maxSize); }

    @Override
    public DistinctStrings add(DistinctStrings left, DistinctStrings right) {
        return left.union(right);
    }

    @Override
    public DistinctStrings create(final ITable data) {
        DistinctStrings result = this.getZero();
        result.addStrings(data.getColumn(this.colName), data.getMembershipSet());
        return result;
    }
}
