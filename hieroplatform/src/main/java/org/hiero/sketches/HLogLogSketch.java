package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;
import org.hiero.utils.Randomness;

import javax.annotation.Nullable;

public class HLogLogSketch implements ISketch<ITable, HLogLog> {
    final String colName;
    final int seed; //seed for the hash function of the HLogLog
    /* the log of the #bytes used by each data structure. Should be in 4...16.
     * More space means more accuracy. A space of 10-14 is recommended. */
    final int spaceBound;
    public HLogLogSketch(String colName) {
        this.colName = colName;
        this.seed = new Randomness().nextInt();
        this.spaceBound = 12;
    }

    public HLogLogSketch(String colName, int spaceBound, int seed) {
        this.colName = colName;
        this.seed = seed;
        if (!HLogLog.spaceValid(spaceBound))
            throw new IllegalArgumentException("HLogLogSketch initialized with number out of range");
        this.spaceBound = spaceBound;
    }

    @Override
    public HLogLog create(final ITable data) {
        HLogLog result = this.getZero();
        result.createHLL(data.getColumn(this.colName), data.getMembershipSet());
        return result;
    }

    @Override
    public HLogLog add(@Nullable final HLogLog left, @Nullable final HLogLog right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public HLogLog zero() {
        return new HLogLog(this.spaceBound, this.seed);
    }
}
