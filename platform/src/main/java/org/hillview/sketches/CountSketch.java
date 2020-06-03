package org.hillview.sketches;

import org.hillview.dataset.IncrementalTableSketch;
import org.hillview.sketches.results.Count;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class CountSketch extends IncrementalTableSketch<Count, EmptyWorkspace> {

    @Override
    public void add(EmptyWorkspace v, Count result, int rowNumber) {
        result.add(1);
    }

    @Override
    public EmptyWorkspace initialize(ITable data) { return EmptyWorkspace.instance; }

    @Nullable
    @Override
    public Count create(@Nullable ITable data) {
        int size = Converters.checkNull(data).getMembershipSet().getSize();
        return new Count(size);
    }

    @Nullable
    @Override
    public Count zero() {
        return new Count();
    }

    @Nullable
    @Override
    public Count add(@Nullable Count left, @Nullable Count right) {
        return new Count(Converters.checkNull(left).count + Converters.checkNull(right).count);
    }
}
