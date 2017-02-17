package org.hiero.sketch.spreadsheet;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;
/**
 * Implements the ITopK interface as a SortedMap (which uses Red-Black trees).
 * Seems faster than HashMap implementation.
 */
public class TreeTopK<T> implements ITopK<T> {
    private final int maxSize;
    private int size;
     private final SortedMap<T, Integer> data;
    private T cutoff; /* max value that currently belongs to Top K. */
     private final Comparator<T> greater;

    public TreeTopK(final int maxSize,  final Comparator<T> greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new TreeMap<T, Integer>(this.greater);
    }

    @Override
    public SortedMap<T, Integer> getTopK() {
        return this.data;
    }

    @Override
    public void push(final T newVal) {
        if (this.size == 0) {
            this.data.put(newVal, 1); // Add newVal to Top K
            this.cutoff = newVal;
            this.size = 1;
            return;
        }
        final int gt = this.greater.compare(newVal, this.cutoff);
        if (gt <= 0) {
            if (this.data.containsKey(newVal)) { //Already in Top K, increase count. Size, cutoff do not change
                final int count = this.data.get(newVal) + 1;
                this.data.put(newVal, count);
            } else { // Add a new key to Top K
                this.data.put(newVal, 1);
                if (this.size >= this.maxSize) {        // Remove the largest key, compute the new largest key
                    this.data.remove(this.cutoff);
                    this.cutoff = this.data.lastKey();
                } else {
                    this.size += 1;
                }
            }
        } else {   // gt equals 1
            if (this.size < this.maxSize) {   // Only case where newVal needs to be added
                this.size += 1;
                this.data.put(newVal, 1);     // Add newVal to Top K
                this.cutoff = newVal;    // It is now the largest value
            }
        }
    }
}
