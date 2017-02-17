package org.hiero.sketch.spreadsheet;
import javax.annotation.Nonnull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import rx.Observable;

public class Hist2DSketch implements ISketch<ITable, Histogram2DHeavy> {
    final IBucketsDescription1D bucketDescD1;
    final IBucketsDescription1D bucketDescD2;
    final String colNameD1;
    final String colNameD2;
    final IStringConverter converterD1;
    final IStringConverter converterD2;
    final double rate;

    public Hist2DSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         IStringConverter converter1, IStringConverter converter2,
                         String colName1, String colName2) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = 1;
    }

    public Hist2DSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         IStringConverter converter1, IStringConverter converter2,
                         String colName1, String colName2, double rate) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = rate;
    }

    public Histogram2DHeavy getHistogram( final ITable data) {
        Histogram2DHeavy result = this.zero();
        result.createHistogram(data.getColumn(this.colNameD1), data.getColumn(this.colNameD2),
                this.converterD1, this.converterD2, data.getMembershipSet().sample(this.rate));
        return result;
    }

    @Override
    public  Histogram2DHeavy zero() {
        return new Histogram2DHeavy(this.bucketDescD1, this.bucketDescD2);
    }

    @Override
    public  Histogram2DHeavy add( final Histogram2DHeavy left,
                                          final Histogram2DHeavy right) {
        return left.union(right);
    }

    @Override
    public  Observable<PartialResult<Histogram2DHeavy>> create(final ITable data) {
        Histogram2DHeavy hist = this.getHistogram(data);
        PartialResult<Histogram2DHeavy> result = new PartialResult<>(1.0, hist);
        return Observable.just(result);
    }
}
