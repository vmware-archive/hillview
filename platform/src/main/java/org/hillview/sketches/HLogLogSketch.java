package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.Randomness;

import javax.annotation.Nullable;

public class HLogLogSketch implements ISketch<ITable, HLogLog> {
    private final String colName;
    private final int seed; //seed for the hash function of the HLogLog
    /**
     * the log of the #bytes used by each data structure. Should be in 4...16.
     * More space means more accuracy. A space of 10-14 is recommended.
     **/
    private final int logSpaceSize;

    public HLogLogSketch(String colName) {
        this.colName = colName;
        this.seed = new Randomness().nextInt();
        this.logSpaceSize = 12;
    }

    public HLogLogSketch(String colName, int logSpaceSize, int seed) {
        this.colName = colName;
        this.seed = seed;
        HLogLog.checkSpaceValid(logSpaceSize);
        this.logSpaceSize = logSpaceSize;
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
        return new HLogLog(this.logSpaceSize, this.seed);
    }
}
