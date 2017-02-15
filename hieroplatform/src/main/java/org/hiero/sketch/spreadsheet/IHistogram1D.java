package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IStringConverter;

public interface IHistogram1D {

    void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                                      final IStringConverter converter, double sampleRate);

    void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                               final IStringConverter converter, double sampleRate, long seed);

    void createHistogram(final IColumn column, final IMembershipSet membershipSet,
                         final IStringConverter converter);

    int getNumOfBuckets();
}
