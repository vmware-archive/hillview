package org.hillview.table.api;

/**
 * An iterator over the rows that were sampled with some rate
 */

public interface ISampledRowIterator extends IRowIterator {
    /**
     * @return the sample rate with which the iterator passes through the membershipSet
     */
    default double rate() { return 1.0; }
}
