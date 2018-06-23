package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 *  A data structure that computes the minHash of a column of a table. It stores the k column values
 *  that hash to the minimum value.
 */
public class MinKSet {
    public final int maxSize;
    public final Long2ObjectRBTreeMap data;

    public MinKSet(int maxSize) {
        this.maxSize = maxSize;
        this.data = new Long2ObjectRBTreeMap();
    }

    public MinKSet(int maxSize, Long2ObjectRBTreeMap data) {
        this.maxSize = maxSize;
        this.data = data;
    }

    /* Currently specialized to string for sorting */
    public List<String> getSamples() {
        List<String> samples = new ArrayList<String>(this.data.values());
        Collections.sort(samples);
        return samples;
    }

    /**
     * This method will return a prescribed number of bucket boundaries.
     * @param numBoundaries The number of boundaries: should be 1 less than the number of buckets.
     * @return An ordered list of boundaries for the buckets. The first bucket starts at min, the
     * last bucket ends at max. All other boundaries are given by this routine.
     */
    public List<String> getBoundaries(int numBoundaries) {
        List<String> samples = this.getSamples();
        int numSamples = samples.size();
        if (numSamples <= numBoundaries)
            return samples;
        List <String> boundaries = new ArrayList<>(numBoundaries);
        for(int i = 1; i <= numBoundaries; i++) {
            int j = (int) Math.ceil(numSamples * i/(numBoundaries + 1.0));
            boundaries.add(samples.get(j));
        }
        return boundaries;
    }
}