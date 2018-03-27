package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * A sketch that estimates whether a column has more than items than a given threshold. It invokes the
 * NumItemsThreshold class.
 */

public class NIThresholdSketch implements ISketch<ITable, NumItemsThreshold> {

    private final String colName;
    /**
     * Seed for the hash function used by NumItemsThreshold
     */
    private final long seed;
    /**
     * The log of the threshold size. The default is 13.
     */
    private final int logThreshold;

    public NIThresholdSketch(String colName, long seed) {
        this(colName, 13, seed);
    }

    public NIThresholdSketch(String colName, int logThreshold, long seed) {
        this.colName = colName;
        this.seed = seed;
        this.logThreshold = logThreshold;
    }

    @Override
    public NumItemsThreshold create(final ITable data) {
        NumItemsThreshold result = this.getZero();
        ColumnAndConverter col = data.getLoadedColumn(this.colName);
        result.createBits(col.column, data.getMembershipSet());
        return result;
    }

    @Override
    public NumItemsThreshold add(@Nullable final NumItemsThreshold left, @Nullable final NumItemsThreshold right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }

    @Override
    public NumItemsThreshold zero() {
        return new NumItemsThreshold(this.logThreshold, this.seed);
    }
}
