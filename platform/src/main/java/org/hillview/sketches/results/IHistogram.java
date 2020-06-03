package org.hillview.sketches.results;

public interface IHistogram {
    long getMissingCount();
    int getBucketCount();
    long getCount(int bucket);
}
