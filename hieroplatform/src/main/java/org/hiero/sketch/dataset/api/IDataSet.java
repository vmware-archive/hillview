package org.hiero.sketch.dataset.api;

import org.checkerframework.checker.nullness.qual.NonNull;
import rx.Observable;

/**
 * A distributed dataset with elements of type T in the leaves.
 * All operations on datasets return observables, which are streams of partial results.
 * Cancellation of an operation is done by unsubscribing from such an observable.
 *
 * @param <T> Type of elements stored in the dataset.
 */
public interface IDataSet<T> {
    /**
     * Run a computation on the dataset, return another dataset.
     * @param mapper  Computation to run on the dataset.
     * @param <S>     Type of result in the result dataset.
     * @return        A stream of partial results (all IDataSet[S]), only one of which should really
     *                be the actual result.  All the other ones should be null.
     */
    @NonNull <S> Observable<PartialResult<IDataSet<S>>> map(@NonNull IMap<T, S> mapper);

    /**
     * Run a sketch on a dataset, returning a value.
     * @param sketch  Sketch computation to run on the dataset.
     * @param <R>     Type of result produced.
     * @return        A stream of partial results, all of type R.  Adding these partial results
     *                will produce the correct final result.  The sketch itself has an 'add' method.
     */
    @NonNull <R> Observable<PartialResult<R>> sketch(@NonNull ISketch<T, R> sketch);

    /**
     * Combine two datasets that have the exact same topology by pairing the values in the
     * corresponding leaves.
     * @param other  Dataset to combine with this.
     * @param <S>    Type of data in the other dataset.
     * @return       A stream of partial results which are all IDataSet[Pair[T,S]].  In fact this
     *               stream will contain exactly one result.
     */
    @NonNull <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(@NonNull IDataSet<S> other);

    // The following are various helper methods.

    /**
     * Extracts the values from a stream of PartialResults.
     * @param results  Stream of partial results.
     * @param <R>      Type of data in partial results.
     * @return         A stream containing just the data of the partial results.
     */
    @NonNull static <R> Observable<R> getValues(
            @NonNull final Observable<PartialResult<R>> results) {
        return results.map(e -> e.deltaValue);
    }

    /**
     * Reduce a stream of values from a monoid.
     * @param data     Stream of values.
     * @param monoid   Monoid that knows how to add values.
     * @param <R>      Type of data to be reduced.
     * @return         A stream containing exactly one value - the sum of all
     *                 values in the data stream.
     */
    @NonNull static <R> Observable<R> reduce(
            @NonNull final Observable<R> data,
            @NonNull final IMonoid<R> monoid) {
        return data.reduce(monoid.zero(), monoid::add);
    }

    @NonNull static <S> Observable<IDataSet<S>> reduce(
            @NonNull final Observable<PartialResult<IDataSet<S>>> results) {
        final IMonoid<PartialResult<IDataSet<S>>> mono = new PRDataSetMonoid<S>();
        return getValues(reduce(results, mono));
    }

    // The convenience methods below should be used only for testing.

    /**
     * Applies a map and then reduces the result immediately.
     * @param mapper  Mapper to apply to data.
     * @param <S>     Type of data in the result.
     * @return        An observable with a single element.
     */
    @NonNull default <S> Observable<IDataSet<S>> singleMap(
            @NonNull final IMap<T, S> mapper) {
        return reduce(this.map(mapper));
    }

    /**
     * Applies a zip and then reduces the result immediately.
     * @param <S>     Type of data in the result.
     * @return        An observable with a single element.
     */
    @NonNull default <S> Observable<IDataSet<Pair<T, S>>> singleZip(
            @NonNull final IDataSet<S> other) {
        return reduce(this.zip(other));
    }

    /**
     * Applies a sketch and then reduces the result immediately.
     * @param sketch  Sketch to apply to the data.
     * @param <R>     Type of data in the result.
     * @return        An observable with a single element.
     */
    @NonNull default <R> Observable<R> singleSketch(
            @NonNull final ISketch<T, R> sketch) {
        return reduce(getValues(this.sketch(sketch)), sketch);
    }

    /**
     * Run a map synchronously.
     * @param mapper  Mapper to run.
     * @param <S>     Type of data produced.
     * @return        An IDataSet containing the final result of the map.
     */
    @NonNull default <S> IDataSet<S> blockingMap(@NonNull final IMap<T, S> mapper) {
        return this.singleMap(mapper).toBlocking().single();
    }

    /**
     * Run a zip synchronously.
     * @param <S>     Type of data produced.
     * @return        An IDataSet containing the final result of the zip.
     */
    @NonNull default <S> IDataSet<Pair<T, S>> blockingZip(@NonNull final IDataSet<S> other) {
        return this.singleZip(other).toBlocking().single();
    }

    /**
     * Run a sketch synchronously.
     * @param sketch  Sketch to run.
     * @param <R>     Type of data produced.
     * @return        An IDataSet containing the final result of the sketch.
     */
    @NonNull default <R> R blockingSketch(@NonNull final ISketch<T, R> sketch) {
        return this.singleSketch(sketch).toBlocking().single();
    }
}
