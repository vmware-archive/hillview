package org.hillview.sketches.results;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.sketches.NextKSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.Noise;

import javax.annotation.Nullable;

public class NextKSketchNoisy extends PostProcessedSketch<ITable, NextKList, NextKList> {
    private final Noise rowCountNoise;

    public NextKSketchNoisy(NextKSketch sketch, Noise noise) {
        super(sketch);
        this.rowCountNoise = noise;
    }

    @Nullable
    @Override
    public NextKList postProcess(@Nullable NextKList r) {
        if (r == null)
            return null;
        if (r.aggregates != null)
            throw new RuntimeException("Aggregates not supported in private views");
        return new NextKList(
                r.rows, null, r.count, r.startPosition,
                r.rowsScanned + Converters.toLong(this.rowCountNoise.getNoise()));
    }
}
