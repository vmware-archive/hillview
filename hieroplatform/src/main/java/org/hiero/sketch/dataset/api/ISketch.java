package org.hiero.sketch.dataset.api;

import org.hiero.utils.Converters;

import java.io.Serializable;

/**
 * Describes a sketch computation on a dataset of type T that produces a result of type R.
 * This class is also a monoid which knows how to combine two values of type R using the add
 * method.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public interface ISketch<T, R> extends Serializable, IMonoid<R> {
    /**
     * Sketch computation on some dataset T.
     * @param data  Data to sketch.
     * @return  A sketch of the data.
     */
    R create(T data);

    /**
     * Helper method to return non-null zeros.
     */
    default R getZero() { return Converters.checkNull(this.zero()); }
}
