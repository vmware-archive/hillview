package org.hiero.sketch.spreadsheet;
import javax.annotation.Nonnull;

import java.util.*;
/**
 * HeapTopK implements the TopK Interface as a HashMap, and only sorts it when asked to return topK.
 * Seems to be slower that the TreeMap implementation
 * Possible reason: Membership and max finding are slower.
 */
public class HeapTopK<T> implements ITopK<T> {
    private final int maxSize;
    private int size;
    @Nonnull
    private final HashMap<T, Integer> data;
    private T cutoff; /* max value that currently belongs to Top K. */
    @Nonnull
    private final Comparator<T> greater;

    public HeapTopK(@Nonnull final int maxSize, @Nonnull final Comparator<T> greater) {
        if (maxSize > 0)
            this.maxSize = maxSize;
        else
            throw new IllegalArgumentException("Size should be positive");
        this.size = 0;
        this.greater = greater;
        this.data = new HashMap<T, Integer>();
    }

    @Override
    public SortedMap<T, Integer> getTopK() {
        final SortedMap<T, Integer> finalMap = new TreeMap<T, Integer>(this.greater);
        finalMap.putAll(this.data);
        return finalMap;
    }

    @Override
    public void push(final T newVal) {
        if (this.size == 0) {
            this.size += 1;
            this.data.put(newVal, 1); // Add newVal to Top K
            this.cutoff = newVal;
            return;
        }
        final int gt = this.greater.compare(newVal, this.cutoff);
        if (gt <= 0) {
            if (this.data.containsKey(newVal)) { //Already in Top K, increase count
                final int count = this.data.get(newVal) + 1;
                this.data.put(newVal, count);
            } else { // Add a new key to Top K
                this.data.put(newVal, 1);
                if (this.size >= this.maxSize) { // Remove the largest key, compute the new largest key
                    this.data.remove(this.cutoff);
                    this.cutoff = Collections.max(this.data.keySet(), this.greater);
                } else
                    this.size += 1;
            }
        }
        else {   //gt equals 1
            if (this.size < this.maxSize) { // Only case where newVal needs to be added
                this.size += 1;
                this.data.put(newVal, 1); // Add newVal to Top K
            }
        }
    }
}