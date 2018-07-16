package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
/**
 *  A data structure that computes the minHash of a column of a table. It stores the k column values
 *  that hash to the minimum value.
 */
public class MinKSet<T> {
    public final Comparator<T> comp;
    public final int maxSize;
    public final Long2ObjectRBTreeMap<T> data;
    public T min;
    public T max;

    public MinKSet(int maxSize, Comparator<T> comp) {
        this.maxSize = maxSize;
        this.comp = comp;
        this.data = new Long2ObjectRBTreeMap();
        this.min = null;
        this.max = null;
    }

    public MinKSet(int maxSize, Long2ObjectRBTreeMap<T> data, Comparator<T> comp, T min, T max) {
        this.comp = comp;
        this.maxSize = maxSize;
        this.data = data;
        this.min = min;
        this.max = max;
    }

    public List<T> getSamples() {
        List<T> samples = new ArrayList<T>(this.data.values());
        Collections.sort(samples, this.comp);
        return samples;
    }

    /**
     * This method will return (at most) a prescribed number of bucket boundaries.
     * @param maxBuckets The maximum number of buckets.
     * @return An ordered list of boundaries for b <= maxBuckets buckets. If the number of distinct
     * strings is small, the number of buckets b could be strictly smaller than maxBuckets.
     * If the number of buckets is b, the number of boundaries is b+1. The first bucket starts at
     * min, the last bucket ends at max. The buckets boundaries are all distinct, hence the number
     * of buckets returned might be smaller.
     */
    public List<T> getBoundaries(int maxBuckets) {
        List<T> samples = this.getSamples();
        samples.remove(this.min);
        samples.remove(this.max);
        int numSamples = samples.size();
        List <T> boundaries = new ArrayList<T>(maxBuckets + 1);
        boundaries.add(this.min);
        if (numSamples <= maxBuckets - 1)
            boundaries.addAll(samples);
        else {
            for (int i = 1; i < maxBuckets; i++) {
                int j = (int) Math.ceil(numSamples * i / ((float) maxBuckets)) -1;
                boundaries.add(samples.get(j));
            }
        }
        boundaries.add(this.max);
        return boundaries;
    }
}