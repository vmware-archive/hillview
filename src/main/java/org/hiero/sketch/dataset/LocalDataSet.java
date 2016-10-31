package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.*;
import rx.Observable;

public class LocalDataSet<T> implements IDataSet<T> {
    private final T data;
    // TODO: run on a separate thread

    public LocalDataSet(final T data) {
        this.data = data;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        final PartialResult<S> initial = new PartialResult<S>(0.0, null);
        final Observable<PartialResult<S>> start = Observable.just(initial);
        final Observable<PartialResult<S>> trsf = mapper.apply(this.data);
        final Observable<PartialResult<S>> chain = start.concatWith(trsf);
        final Observable<PartialResult<IDataSet<S>>> progress =
                chain.map(e -> new PartialResult<IDataSet<S>>(e.deltaDone, null));
        final PartialResultMonoid<S> mono = new PartialResultMonoid<S>(new NullMonoid<S>());
        final Observable<PartialResult<S>> last = chain.reduce(mono.zero(), mono::add);
        return progress.concatWith(last.map(e ->
                new PartialResult<IDataSet<S>>(0.0, new LocalDataSet<S>(e.deltaValue))));
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        if (!(other instanceof LocalDataSet<?>))
            throw new RuntimeException("Unexpected type in Zip " + other);
        final LocalDataSet<S> lds = (LocalDataSet<S>) other;
        final Pair<T, S> data = new Pair<T, S>(this.data, lds.data);
        final LocalDataSet<Pair<T, S>> retval = new LocalDataSet<Pair<T, S>>(data);
        return Observable.just(new PartialResult<IDataSet<Pair<T, S>>>(1.0, retval));
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        return sketch.create(this.data);
    }

    @Override
    public String toString() {
        return "LocalDataSet " + this.data.toString();
    }
}
