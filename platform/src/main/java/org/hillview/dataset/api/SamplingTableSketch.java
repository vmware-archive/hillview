package org.hillview.dataset.api;

import org.hillview.dataset.IncrementalTableSketch;
import org.hillview.dataset.TableSketch;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * Runs an incremental sketch over a table with specified sampling parameters.
 * @param <R>  Result produced by the original and this sketch.
 * @param <S>  Actual sketch that will be run.
 */
public class SamplingTableSketch<
        SW extends ISketchWorkspace,
        R extends ISketchResult,
        S extends IncrementalTableSketch<R, SW>>
    implements TableSketch<R> {
    protected final double samplingRate;
    protected final long seed;
    protected final S actualSketch;

    public SamplingTableSketch(double samplingRate, long seed, S actualSketch) {
        this.samplingRate = samplingRate;
        this.seed = seed;
        this.actualSketch = actualSketch;
    }

    @Override
    public R create(@Nullable ITable data) {
        R result = Converters.checkNull(this.actualSketch.zero());
        SW workspace = this.actualSketch.initialize(Converters.checkNull(data));
        ISampledRowIterator it = data
                .getMembershipSet()
                .getIteratorOverSample(this.samplingRate, this.seed, false);
        int row = it.getNextRow();
        while (row >= 0) {
            this.actualSketch.add(workspace, result, row);
            row = it.getNextRow();
        }
        return result;
    }

    @Nullable
    @Override
    public R zero() {
        return this.actualSketch.zero();
    }

    @Nullable
    @Override
    public R add(@Nullable R left, @Nullable R right) {
        return this.actualSketch.add(left, right);
    }
}
