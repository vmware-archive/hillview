package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

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
    public DistinctStrings add(@Nullable DistinctStrings left, @Nullable DistinctStrings right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public DistinctStrings create(final ITable data) {
        DistinctStrings result = this.getZero();
        result.addStrings(data.getColumn(this.colName), data.getMembershipSet());
        return result;
    }
}
