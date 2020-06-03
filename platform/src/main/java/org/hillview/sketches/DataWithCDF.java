package org.hillview.sketches;

import org.hillview.dataset.PostProcessedSketch;
import org.hillview.dataset.TableSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.Histogram;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * This post-processed sketch integrates the second argument as post-processing.
 * @param <D>  Type of first argument.
 */
public class DataWithCDF<D> extends
        PostProcessedSketch<ITable, Pair<D, Histogram>, Pair<D, Histogram>> {
    public DataWithCDF(ISketch<ITable, Pair<D, Histogram>> sketch) {
        super(sketch);
    }

    @Nullable
    @Override
    public Pair<D, Histogram> postProcess(@Nullable Pair<D, Histogram> result) {
        D first = Converters.checkNull(Converters.checkNull(result).first);
        Histogram cdf = Converters.checkNull(result.second);
        return new Pair<D, Histogram>(first, cdf.integrate());
    }
}
