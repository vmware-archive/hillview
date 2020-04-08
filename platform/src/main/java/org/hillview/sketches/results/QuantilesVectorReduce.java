package org.hillview.sketches.results;

import org.hillview.dataset.PostProcessedSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

public class QuantilesVectorReduce extends PostProcessedSketch<ITable, QuantilesVector, QuantilesVector> {
    public QuantilesVectorReduce(ISketch<ITable, QuantilesVector> sketch) {
        super(sketch);
    }

    @Nullable
    @Override
    public QuantilesVector postProcess(@Nullable QuantilesVector result) {
        return null;
    }
}
