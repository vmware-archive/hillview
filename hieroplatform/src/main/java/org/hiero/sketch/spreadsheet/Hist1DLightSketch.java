package org.hiero.sketch.spreadsheet;
import javax.annotation.Nonnull;
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
    public Histogram1DLight create(final ITable data) {
        Histogram1DLight result = this.zero();
        result.createHistogram(data.getColumn(this.colName),
                    data.getMembershipSet().sample(this.rate), this.converter);
        return result;
    }

    @Override
    public Histogram1DLight add(final Histogram1DLight left,
                                final Histogram1DLight right) {
        return left.union(right);
    }

    @Override
    public Histogram1DLight zero() {
        return new Histogram1DLight(this.bucketDesc);
    }
}
