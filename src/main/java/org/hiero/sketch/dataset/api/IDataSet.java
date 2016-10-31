package org.hiero.sketch.dataset.api;

import rx.Observable;

/**
 * A distributed dataset with elements of type T in the leaves.
 * Cancellation of an operation is deltaDone by unsubscribing from
 * an observable.
 *
 * @param <T> Type of elements stored in the dataset.
 */
public interface IDataSet<T> {
    <S> Observable<PartialResult<IDataSet<S>>> map(IMap<T, S> mapper);

    <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(IDataSet<S> other);

    <R> Observable<PartialResult<R>> sketch(ISketch<T, R> sketch);

    static <R> Observable<R> getValues(final Observable<PartialResult<R>> results) {
        return results.map(e -> e.deltaValue);
    }

    static <R> Observable<PartialResult<R>> reducePR(final Observable<PartialResult<R>> results,
                                                     final IMonoid<PartialResult<R>> monoid) {
        return results.reduce(monoid.zero(), monoid::add);
    }

    static <R> Observable<R> reduce(final Observable<PartialResult<R>> results, final IMonoid<R> monoid) {
        final PartialResultMonoid<R> mono = new PartialResultMonoid<R>(monoid);
        return getValues(reducePR(results, mono));
    }

    static <S> Observable<IDataSet<S>> reduce(final Observable<PartialResult<IDataSet<S>>> results) {
        final IMonoid<PartialResult<IDataSet<S>>> mono = new PRDataSetMonoid<S>();
        return getValues(reducePR(results, mono));
    }

    default <S> Observable<IDataSet<S>> simpleMap(final IMap<T, S> mapper) {
        return reduce(this.map(mapper));
    }

    default <S> Observable<IDataSet<Pair<T, S>>> simpleZip(final IDataSet<S> other) {
        return reduce(this.zip(other));
    }

    default <R> Observable<R> simpleSketch(final ISketch<T, R> sketch) {
        return reduce(this.sketch(sketch), sketch);
    }

    default <S> IDataSet<S> blockingMap(final IMap<T, S> mapper) {
        return this.simpleMap(mapper).toBlocking().single();
    }

    default <S> IDataSet<Pair<T, S>> blockingZip(final IDataSet<S> other) {
        return this.simpleZip(other).toBlocking().single();
    }

    default <R> R blockingSketch(final ISketch<T, R> sketch) {
        return this.simpleSketch(sketch).toBlocking().single();
    }
}
