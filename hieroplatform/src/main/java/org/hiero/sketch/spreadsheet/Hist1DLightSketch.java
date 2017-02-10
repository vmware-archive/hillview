package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import rx.Observable;

public class Hist1DLightSketch implements ISketch<ITable, Histogram1DLight> {

    final IBucketsDescription1D bucketDesc;
    final String colName;
    final IStringConverter converter;
    final double rate;

    public Histogram1DLight getHistogram(ITable data){
        Histogram1DLight result = this.zero();
        if (this.rate == 1)
            result.createHistogram(data.getColumn(this.colName),
                    data.getMembershipSet(), this.converter);
        else
            result.createHistogram(data.getColumn(this.colName),
                    data.getMembershipSet().sample(this.rate), this.converter);
        return result;
    }

    public Hist1DLightSketch(IBucketsDescription1D bucketDesc, String colName, IStringConverter converter) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
    }

    public Hist1DLightSketch(IBucketsDescription1D bucketDesc, String colName,
                             IStringConverter converter, double rate) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
    }

    @Override
    public Histogram1DLight add(@NonNull final Histogram1DLight left,
                                @NonNull final Histogram1DLight right) {
        return left.union(right);
    }

    @Override
    public Histogram1DLight zero() {
        return new Histogram1DLight(this.bucketDesc);
    }

    @Override
    public Observable<PartialResult<Histogram1DLight>> create(final ITable data) {
        Histogram1DLight hist = this.getHistogram(data);
        PartialResult<Histogram1DLight> result = new PartialResult<>(1.0, hist);
        return Observable.just(result);
    }
}
