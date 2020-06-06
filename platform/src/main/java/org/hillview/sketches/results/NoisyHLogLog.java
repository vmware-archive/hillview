package org.hillview.sketches.results;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.sketches.HLogLogSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.CountWithConfidence;
import org.hillview.utils.Noise;

import javax.annotation.Nullable;

public class NoisyHLogLog extends PostProcessedSketch<ITable, HLogLog, CountWithConfidence> {
    private final Noise noise;

    public NoisyHLogLog(HLogLogSketch sketch, Noise noise) {
        super(sketch);
        this.noise = noise;
    }

    @Nullable
    @Override
    public CountWithConfidence postProcess(@Nullable HLogLog result) {
        return Converters.checkNull(result).getCount().add(this.noise);
    }
}
