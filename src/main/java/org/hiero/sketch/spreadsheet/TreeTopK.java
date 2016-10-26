package org.hiero.sketch.spreadsheet;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implements the ITopK interface as a SortedMap (which uses Red-Black trees).
 * Seems faster than HashMap implementation.
 */

public class TreeTopK<T> implements ITopK<T>{

    private int maxSize, size;
    private SortedMap<T, Integer> data;
    private T cutoff; /* max value that currently belongs to Top K. */
    private Comparator<T> greater;

    public TreeTopK(int maxSize, Comparator<T> greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new TreeMap<T, Integer>();
    }
    @Override
    public SortedMap<T,Integer> getTopK() {
        return data;
    }

    @Override
    public void push(T newVal) {
        if (size == 0) {
            data.put(newVal, 1); // Add newVal to Top K
            cutoff = newVal;
            size = 1;
            return;
        }
        int gt = greater.compare(newVal, cutoff);
        if(gt <= 0)
            if (data.containsKey(newVal)) { //Already in Top K, increase count. Size, cutoff do not change
                int count = data.get(newVal) + 1;
                data.put(newVal, count);
            }
            else { // Add a new key to Top K
                data.put(newVal, 1);
                if (size >= maxSize){        // Remove the largest key, compute the new largest key
                    data.remove(cutoff);
                    cutoff = data.lastKey();
                }
                else
                    size += 1;
            }
        else    // gt equals 1
            if (size < maxSize) {   // Only case where newVal needs to be added
                size += 1;
                data.put(newVal, 1);     // Add newVal to Top K
                cutoff = newVal;    // It is now the largest value
            }
    }
}
