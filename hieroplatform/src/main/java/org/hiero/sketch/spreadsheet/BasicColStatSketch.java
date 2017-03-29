package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;
import javax.annotation.Nullable;

public class BasicColStatSketch implements ISketch<ITable, BasicColStats> {
    final String colName;
    @Nullable
    final IStringConverter converter;
    final double rate;
    final int momentNum;

    public BasicColStatSketch(String colName, @Nullable IStringConverter converter) {
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
        this.momentNum = 2;
    }

    public BasicColStatSketch(String colName, @Nullable IStringConverter converter,
                              int momentNum, double rate) {
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
        this.momentNum = momentNum;
    }

    @Override
    public BasicColStats create(final ITable data) {
        BasicColStats result = this.getZero();
        result.createStats(data.getColumn(this.colName), data.getMembershipSet().sample(this.rate),
                this.converter);
        return result;
    }

    @Override
    public BasicColStats zero() { return new BasicColStats(this.momentNum); }

    @Override
    public BasicColStats add(@Nullable final BasicColStats left, @Nullable final BasicColStats right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
