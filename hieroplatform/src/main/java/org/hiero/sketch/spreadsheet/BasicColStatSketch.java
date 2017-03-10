package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

public class BasicColStatSketch implements ISketch<ITable, BasicColStat> {
    final String colName;
    @Nullable
    final IStringConverter converter;
    final double rate;
    final int momentNum;

    public BasicColStatSketch( String colName, @Nullable IStringConverter converter) {
        this.colName = colName;
        this.converter = converter;
        this.rate = 1;
        this.momentNum = 2;
    }

    public BasicColStatSketch( String colName, @Nullable IStringConverter converter,
                               int momentNum, double rate) {
        this.colName = colName;
        this.converter = converter;
        this.rate = rate;
        this.momentNum = 2;
    }

    @Override
    public BasicColStat create(final ITable data) {
        BasicColStat result = this.getZero();
        result.createStats(data.getColumn(this.colName), data.getMembershipSet().sample(this.rate),
                this.converter);
        return result;
    }

    @Override
    public BasicColStat zero() {
        return new BasicColStat(momentNum);
    }

    @Override
    public BasicColStat add(@Nullable final BasicColStat left, @Nullable final BasicColStat right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
