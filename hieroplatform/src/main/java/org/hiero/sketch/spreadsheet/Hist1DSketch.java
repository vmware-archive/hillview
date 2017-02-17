package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

public class Hist1DSketch implements ISketch<ITable, Histogram1D> {
    final IBucketsDescription1D bucketDesc;
    final String colName;
    @Nullable
    final IStringConverter converter;
    final double rate;

    public Hist1DSketch(IBucketsDescription1D bucketDesc, String colName, IStringConverter converter) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
    }

    public Hist1DSketch(IBucketsDescription1D bucketDesc, String colName,
                        @Nullable IStringConverter converter, double rate) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
    }

    @Override @Nullable
    public Histogram1D create(final ITable data) {
        Histogram1D result = this.getZero();
        result.createHistogram(data.getColumn(this.colName),
                data.getMembershipSet().sample(this.rate), this.converter);
        return result;
    }

    @Override @Nullable
    public Histogram1D add(@Nullable final Histogram1D left, @Nullable final Histogram1D right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public Histogram1D zero() {
        return new Histogram1D(this.bucketDesc);
    }
}
