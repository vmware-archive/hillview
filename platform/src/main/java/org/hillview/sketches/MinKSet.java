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

    public MinKSet(int maxSize, Comparator<T> comp) {
        this.maxSize = maxSize;
        this.comp = comp;
        this.data = new Long2ObjectRBTreeMap();
    }

    public MinKSet(int maxSize, Long2ObjectRBTreeMap<T> data, Comparator<T> comp) {
        this.comp = comp;
        this.maxSize = maxSize;
        this.data = data;
    }

    /* Currently specialized to string for sorting */
    public List<T> getSamples() {
        List<T> samples = new ArrayList<T>(this.data.values());
        Collections.sort(samples, this.comp);
        return samples;
    }

    /**
     * This method will return a prescribed number of bucket boundaries. The first bucket starts at
     * min, the last bucket ends at max. All other boundaries are given by this routine. Hence the
     * number of boundaries should be 1 less than the number of buckets.
     * @param numBoundaries The number of boundaries.
     * @return An ordered list of boundaries for the buckets.
     */
    public List<T> getBoundaries(int numBoundaries) {
        List<T> samples = this.getSamples();
        int numSamples = samples.size();
        if (numSamples <= numBoundaries)
            return samples;
        List <T> boundaries = new ArrayList<T>(numBoundaries);
        for(int i = 1; i <= numBoundaries; i++) {
            int j = (int) Math.ceil(numSamples * i/(numBoundaries + 1.0));
            boundaries.add(samples.get(j));
        }
        return boundaries;
    }
}