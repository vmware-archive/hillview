package org.hillview.dataset;

import org.hillview.dataset.api.ISketch;

import javax.annotation.Nullable;

/**
 * A sketch with post processing that applies the identify function for post processing.
 * @param <T>   Input sketch data.
 * @param <R>   Output sketch data.
 */
public class IdPostProcessedSketch<T, R> extends PostProcessedSketch<T, R, R> {
    public IdPostProcessedSketch(ISketch<T, R> sketch) {
        super(sketch);
    }

    @Override
    @Nullable
    public R postProcess(@Nullable R data) { return data; }
}
