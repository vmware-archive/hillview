package org.hiero.sketch.spreadsheet;

import javax.annotation.Nonnull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import rx.Observable;

public class Hist1DSketch implements ISketch<ITable, Histogram1D> {
    final IBucketsDescription1D bucketDesc;
    final String colName;
    final IStringConverter converter;
    final double rate;

    public Hist1DSketch(IBucketsDescription1D bucketDesc, String colName, IStringConverter converter) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
    }

    public Hist1DSketch(IBucketsDescription1D bucketDesc, String colName,
                        IStringConverter converter, double rate) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
    }

    @Override
    public Histogram1D create(final ITable data) {
        Histogram1D result = this.zero();
        result.createHistogram(data.getColumn(this.colName),
                data.getMembershipSet().sample(this.rate), this.converter);
        return result;
    }

    @Override
    public Histogram1D add(final Histogram1D left,final Histogram1D right) {
        return left.union(right);
    }

    @Override
    public Histogram1D zero() {
        return new Histogram1D(this.bucketDesc);
    }
}
