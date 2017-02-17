package org.hiero.sketch.spreadsheet;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

public class HeatMapSketch implements ISketch<ITable, HeatMap> {
    final IBucketsDescription1D bucketDescD1;
    final IBucketsDescription1D bucketDescD2;
    final String colNameD1;
    final String colNameD2;
    @Nullable final IStringConverter converterD1;
    @Nullable final IStringConverter converterD2;
    final double rate;

    public HeatMapSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         @Nullable IStringConverter converter1, @Nullable IStringConverter converter2,
                         String colName1, String colName2) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = 1;
    }

    public HeatMapSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         @Nullable IStringConverter converter1, @Nullable IStringConverter converter2,
                         String colName1, String colName2, double rate) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = rate;
    }

    @Override
    public HeatMap create(final ITable data) {
        HeatMap result = this.getZero();
        result.createHistogram(data.getColumn(this.colNameD1), data.getColumn(this.colNameD2),
                this.converterD1, this.converterD2, data.getMembershipSet().sample(this.rate));
        return result;
    }

    @Override
    public HeatMap zero() {
        return new HeatMap(this.bucketDescD1, this.bucketDescD2);
    }

    @Override
    public HeatMap add(@Nullable final HeatMap left, @Nullable final HeatMap right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
