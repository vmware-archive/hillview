package org.hiero.sketch.spreadsheet;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

public class Hist1DLightSketch implements ISketch<ITable, Histogram1DLight> {
    final IBucketsDescription1D bucketDesc;
    final String colName;
    @Nullable
    final IStringConverter converter;
    final double rate;

    public Hist1DLightSketch(IBucketsDescription1D bucketDesc, String colName,
                             @Nullable IStringConverter converter) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
    }

    public Hist1DLightSketch(IBucketsDescription1D bucketDesc, String colName,
                             @Nullable IStringConverter converter, double rate) {
        this.bucketDesc = bucketDesc;
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
    }

    @Override
    public Histogram1DLight create(final ITable data) {
        Histogram1DLight result = this.getZero();
        result.createHistogram(data.getColumn(this.colName),
                               data.getMembershipSet().sample(this.rate), this.converter);
        return result;
    }

    @Override
    public Histogram1DLight add(@Nullable final Histogram1DLight left,
                                @Nullable final Histogram1DLight right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public Histogram1DLight zero() {
        return new Histogram1DLight(this.bucketDesc);
    }
}
