package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.NumItemsThreshold;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch that estimates whether a column has more than items than a given threshold. It invokes the
 * NumItemsThreshold class.
 */

public class NIThresholdSketch implements ISketch<ITable, NumItemsThreshold> {
    static final long serialVersionUID = 1;
    
    private final String colName;
    /**
     * Seed for the hash function used by NumItemsThreshold
     */
    private final long seed;
    /**
     * The log of the threshold size. The default is 13.
     */
    private final int logThreshold;

    public NIThresholdSketch(String colName, int logThreshold, long seed) {
        this.colName = colName;
        this.seed = seed;
        this.logThreshold = logThreshold;
    }

    @Override
    public NumItemsThreshold create(@Nullable final ITable data) {
        NumItemsThreshold result = this.getZero();
        IColumn col = Converters.checkNull(data).getLoadedColumn(this.colName);
        Converters.checkNull(result).createBits(col, data.getMembershipSet());
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
