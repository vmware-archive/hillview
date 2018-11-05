package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
/**
 *  A data structure that computes the minHash of a column of a table.
 *  It stores the k column values
 *  that hash to the minimum value.
 */
public class MinKSet<T> extends BucketsInfo {
    private final Comparator<T> comp;
    final int maxSize;
    final Long2ObjectRBTreeMap<T> data;
    @Nullable public T min;
    @Nullable public T max;

    MinKSet(int maxSize, Comparator<T> comp) {
        this.maxSize = maxSize;
        this.comp = comp;
        this.data = new Long2ObjectRBTreeMap<T>();
        this.min = null;
        this.max = null;
        this.presentCount = 0;
        this.missingCount = 0;
    }

    MinKSet(int maxSize, Long2ObjectRBTreeMap<T> data, Comparator<T> comp,
            @Nullable T min, @Nullable T max, long numPresent, long numMissing) {
        this.comp = comp;
        this.maxSize = maxSize;
        this.data = data;
        this.min = min;
        this.max = max;
        this.presentCount = numPresent;
        this.missingCount = numMissing;
    }

    public List<T> getSamples() {
        List<T> samples = new ArrayList<T>(this.data.values());
        samples.sort(this.comp);
        return samples;
    }

    public int size() {
        return this.data.size();
    }

    /**
     * Returns true if we have fewer or equal strings to the number of buckets.
     * @param buckets Number of buckets we want.
     */
    public boolean allStringsKnown(int buckets) {
        if (this.min == null)
            // no non-null values
            return true;
        return this.data.size() <= buckets;
    }

    /**
     * Rescale an index from 0-maxBuckets to 0-totalElements.
     * Rank 0 is mapped to 0, rank maxBuckets is mapped to totalElements.
     * The other values of rank are interpolated in between.
     * @param rank            Rank of an element between 0 and maxBuckets.
     * @param maxBuckets      Number of ranks desired.
     * @param totalElements   Number of elements from which we extract the rank.
     */
    public static int getIntegerRank(int rank, int maxBuckets, int totalElements) {
        return (int) Math.ceil(totalElements * rank / ((double)maxBuckets));
    }

    /**
     * This method will return (at most) a prescribed number of left bucket boundaries.
     * @param maxBuckets The maximum number of buckets.
     * @return An ordered list of boundaries for b <= maxBuckets buckets. If the number of distinct
     * strings is small, the number of buckets b could be strictly smaller than maxBuckets.
     * If the number of buckets is b, the number of boundaries is also b. The first bucket starts at
     * min.  The buckets boundaries are all distinct, hence the number
     * of buckets returned might be smaller.
     */
    public JsonList<T> getLeftBoundaries(int maxBuckets) {
        if (this.min == null) {
            // No non-null values
            JsonList <T> boundaries = new JsonList<T>(1);
            boundaries.add(null);
            return boundaries;
        }
        List<T> samples = this.getSamples();
        samples.remove(this.min);
        int numSamples = samples.size();
        JsonList <T> boundaries = new JsonList<T>(maxBuckets);
        boundaries.add(this.min);
        if (numSamples <= maxBuckets - 1) {
            boundaries.addAll(samples);
        } else {
            for (int i = 1; i < maxBuckets; i++) {
                int j = getIntegerRank(i, maxBuckets, numSamples);
                // The samples list does not contain the minimum anymore.
                boundaries.add(samples.get(j - 1));
            }
        }
        return boundaries;
    }
}
