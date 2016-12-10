package org.hiero.sketch.spreadsheet;

import java.util.SortedMap;

/**
 * Interface for computing the topK elements of a data set, ordered by a comparator.
 * getTopK returns a SortedMap of the top K elements.
 * push tries to add a new value newVal to the data structure. This requires
 * - Membership: is it already present?
 * - Maximum: If not present, compare to the Maximum value currently in the Top K
 * - Insert: If we need to Insert newVal
 */
public interface ITopK<T> {
    SortedMap<T,Integer> getTopK();
    void push(T newVal);
}
