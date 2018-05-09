package org.hillview.sketches;

import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import java.util.BitSet;

/**
 * A class that estimates whether the number of distinct items in a column is smaller than a threshold. Implemented by hashing
 * items into a bit array. Once enough bits are set the threshold had been crossed and the iterator may stop
 */
public class NumItemsThreshold implements IJson {
    private final int logThreshold;
    private final int threshold;
    private final int maxLogThreshold = 15;
    private final BitSet bits;
    private final long seed;
    /**
     * The threshold in terms of number of set bits
     */
    private final int bitThreshold;
    /**
     * log the size of the bitSet
     */
    private final int logSize;

    public NumItemsThreshold(int logThreshold, long seed) {
        if ((logThreshold < 1) || (logThreshold > maxLogThreshold))
            throw new IllegalArgumentException("NumItemsThreshold called with illegal size of " + logThreshold);
        this.seed = seed;
        this.logThreshold = logThreshold;
        this.threshold = 1 << logThreshold;
        if (threshold >= 1024) {
            logSize = logThreshold;
            bits = new BitSet(threshold);
            // When the number of bits is equal to the threshold we expect 1-1/e = 0.6322 of the bits to be set. On top
            // of that we add sqrt of the threshold for a high probability bound.
            bitThreshold = (int) Math.round(0.6322 * threshold + Math.sqrt(threshold));
        } else {  // if the threshold is small we want the bitSet still to be large enough to provide sufficient accuracy
            logSize = 10;
            bits = new BitSet(1024);
            double expo = -threshold / 1024.0;
            bitThreshold = (int) Math.round(((1 - Math.pow(2.7182, expo)) * 1024) + Math.sqrt(threshold));
        }
    }

    public NumItemsThreshold(long seed) { this(13, seed); }

    public void createBits(IColumn column, IMembershipSet memSet) {
        final IRowIterator myIter = memSet.getIterator();
        LongHashFunction hash = LongHashFunction.xx(this.seed);
        int currRow = myIter.getNextRow();
        int cardinality = 0;
        while ((currRow >= 0) && (cardinality < bitThreshold)) { // if threshold reached stop iterating
            if (!column.isMissing(currRow)) {
                long itemHash = column.hashCode64(currRow, hash);
                int index =  (int) itemHash >>> (Long.SIZE - this.logSize);
                if (!bits.get(index))
                    cardinality++;
                this.bits.set(index);
            }
            currRow = myIter.getNextRow();
        }
    }

    public NumItemsThreshold union(NumItemsThreshold otherNIT) {
        NumItemsThreshold result = new NumItemsThreshold(this.logThreshold, this.seed);
        result.bits.or(this.bits);
        result.bits.or(otherNIT.bits);
        return result;
    }

    public boolean exceedThreshold() {
        return (bits.cardinality() >= bitThreshold);
    }
}
