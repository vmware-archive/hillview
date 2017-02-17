package org.hiero.sketch.dataset.api;

import javax.annotation.Nonnull;
import rx.Observable;

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
     * The zero of the monoid: the result obtained applying the sketch to some empty dataset T.
     * @return The zero element of the monoid.
     */
    R zero();

    /**
     * The addition function of the monoid.  Addition must be associative, symmetric, and
     * zero must be its neutral element.
     * @param left   Left value to add.
     * @param right  Right value to add.
     * @return       The result of the addition.
     */
    R add(R left, R right);

    /**
     * Sketch computation on some dataset T.
     * @param data  Data to sketch.
     * @return  An observable containing a sequence of sketches; adding these sketches
     * produces the sketch over the complete data.
     */
     Observable<PartialResult<R>> create(T data);

    /**
     * Packages some data in an Observable of a PartialResult.
     * @param data Data; usually the result of the sketch computation.
     * @return An observable which contains exactly one partial result containing the whole data.
     */
     default Observable<PartialResult<R>> pack( final R data) {
        PartialResult<R> pr = new PartialResult<R>(data);
        return Observable.just(pr);
    }
}
