package org.hillview.dataset;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketch;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * A sketch bundled with a post-processing function.
 * @param <T>  Sketch input data.
 * @param <R>  Sketch output data.
 * @param <F>  Data produced after postprocessing.
 */
public abstract class PostProcessedSketch<T, R extends Serializable, F extends IJson> {
    public final ISketch<T, R> sketch;

    protected PostProcessedSketch(ISketch<T, R> sketch) {
        this.sketch = sketch;
    }

    @Nullable
    public abstract F postProcess(@Nullable R result);
}
