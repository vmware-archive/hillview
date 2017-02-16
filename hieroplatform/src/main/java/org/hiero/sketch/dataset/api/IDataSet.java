package org.hiero.sketch.dataset.api;

import javax.annotation.Nonnull;
import rx.Observable;

/**
 * A distributed dataset with elements of type T in the leaves.
 * Cancellation of an operation is deltaDone by unsubscribing from
 * an observable.
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
    <S> Observable<PartialResult<IDataSet<S>>> map(@Nonnull IMap<T, S> mapper);

    <R> Observable<PartialResult<R>> sketch(@Nonnull ISketch<T, R> sketch);

    <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(@Nonnull IDataSet<S> other);

    static <R> Observable<R> getValues(@Nonnull final Observable<PartialResult<R>> results) {
        return results.map(e -> e.deltaValue);
    }

    static <R> Observable<PartialResult<R>> reducePR(
            @Nonnull final Observable<PartialResult<R>> results,
            @Nonnull final IMonoid<PartialResult<R>> monoid) {
        return results.reduce(monoid.zero(), monoid::add);
    }

    static <R> Observable<R> reduce(
            @Nonnull final Observable<PartialResult<R>> results,
            @Nonnull final IMonoid<R> monoid) {
        final PartialResultMonoid<R> mono = new PartialResultMonoid<R>(monoid);
        return getValues(reducePR(results, mono));
    }

    static <S> Observable<IDataSet<S>> reduce(
            @Nonnull final Observable<PartialResult<IDataSet<S>>> results) {
        final IMonoid<PartialResult<IDataSet<S>>> mono = new PRDataSetMonoid<S>();
        return getValues(reducePR(results, mono));
    }

    /**
     * Applies a map and then reduces the result immediately.
     * @param mapper  Mapper to apply to data.
     * @param <S>     Type of data in the result.
     * @return   An observable with a single element.
     */
    default <S> Observable<IDataSet<S>> singleMap(@Nonnull final IMap<T, S> mapper) {
        return reduce(this.map(mapper));
    }

    default <S> Observable<IDataSet<Pair<T, S>>> singleZip(@Nonnull final IDataSet<S> other) {
        return reduce(this.zip(other));
    }

    default <R> Observable<R> singleSketch(@Nonnull final ISketch<T, R> sketch) {
        return reduce(this.sketch(sketch), sketch);
    }

    /*
     * The blocking methods should be used only for testing.
     */
    default <S> IDataSet<S> blockingMap(@Nonnull final IMap<T, S> mapper) {
        return this.singleMap(mapper).toBlocking().single();
    }

    default <S> IDataSet<Pair<T, S>> blockingZip(@Nonnull final IDataSet<S> other) {
        return this.singleZip(other).toBlocking().single();
    }

    default <R> R blockingSketch(@Nonnull final ISketch<T, R> sketch) {
        return this.singleSketch(sketch).toBlocking().single();
    }
}
