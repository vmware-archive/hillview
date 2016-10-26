package org.hiero.sketch.spreadsheet;

import java.util.*;

/**
 * HeapTopK implements the TopK Interface as a HashMap.
 * Seems to be slower that the TreeMap implementation
 * Possible reason: Membership and max finding are slower.
 */

public class HeapTopK<T> implements ITopK<T> {
    private int maxSize, size;
    private HashMap<T, Integer> data;
    private T cutoff; /* max value that currently belongs to Top K. */
    private Comparator<T> greater;

    public HeapTopK(int maxSize, Comparator<T> greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new HashMap<T, Integer>();
    }

    @Override
    public SortedMap<T, Integer> getTopK() {
        SortedMap<T, Integer> finalMap = new TreeMap<T, Integer>(greater);
        finalMap.putAll(data);
        return finalMap;
    }

    @Override
    public void push(T newVal) {
        if (size == 0) {
            size += 1;
            data.put(newVal, 1); // Add newVal to Top K
            cutoff = newVal;
            return;
        }
        int gt = greater.compare(newVal, cutoff);
        if (gt <= 0)
            if (data.containsKey(newVal)) { //Already in Top K, increase count
                int count = data.get(newVal) + 1;
                data.put(newVal, count);
            } else { // Add a new key to Top K
                data.put(newVal, 1);
                if (size >= maxSize) { // Remove the largest key, compute the new largest key
                    data.remove(cutoff);
                    cutoff = Collections.max(data.keySet(), greater);
                } else
                    size += 1;
            }
        else    //gt equals 1
            if (size < maxSize) { // Only case where newVal needs to be added
                size += 1;
                data.put(newVal, 1); // Add newVal to Top K
            }
    }
}