package org.hiero.sketches;

import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.IStringConverter;

import javax.annotation.Nullable;

public class NormalizeCol {
    private double mean;
    private double stDev;

    public NormalizeCol(double mean, double stDev) {
        this.mean = mean;
        this.stDev= stDev;
    }

    public int aboveThreshold(final IColumn column, final IMembershipSet membershipSet,
                            @Nullable final IStringConverter converter, final double threshold) {
        final IRowIterator rowIt = membershipSet.getIterator();
        int row = rowIt.getNextRow();
        int count = 0;
        while (row != -1)
            if (Math.abs(column.asDouble(row, converter) - mean) >= (threshold * stDev)) count++;
        return count;
    }
}
