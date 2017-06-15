package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import javax.annotation.Nullable;

public class BasicColStatSketch implements ISketch<ITable, BasicColStats> {
    private final String colName;
    @Nullable
    private final IStringConverter converter;
    private final double rate;
    private final int momentNum;

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
