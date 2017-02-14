package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.*;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.Callable;

public class LocalDataSet<T> implements IDataSet<T> {
    private final T data;
    private final boolean separateThread;

    public LocalDataSet(final T data) {
        this.data = data;
        this.separateThread = true;
    }

    public LocalDataSet(final T data, final boolean separateThread) {
        this.data = data;
        this.separateThread = separateThread;
    }

    private <R> Observable<PartialResult<R>> zero(Callable<R> z) {
        Observable<R> zero = Observable.fromCallable(z);
        Observable<PartialResult<R>> result = zero.map(e-> new PartialResult(0.0, e));
        return result;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        // Callables provide lazy evaluation
        Observable<PartialResult<IDataSet<S>>> zero = zero(() -> { return null; });
        Callable<IDataSet<S>> r = () -> new LocalDataSet<S>(mapper.apply(this.data));
        Observable<IDataSet<S>> start = Observable.fromCallable(r);
        Observable<PartialResult<IDataSet<S>>> data = start.map(PartialResult::new);
        Observable<PartialResult<IDataSet<S>>>result = zero.concatWith(data);
        if (this.separateThread)
            result = result.observeOn(Schedulers.computation());
        return result;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        if (!(other instanceof LocalDataSet<?>))
            throw new RuntimeException("Unexpected type in Zip " + other);
        LocalDataSet<S> lds = (LocalDataSet<S>) other;
        Pair<T, S> data = new Pair<T, S>(this.data, lds.data);
        LocalDataSet<Pair<T, S>> retval = new LocalDataSet<Pair<T, S>>(data);
        return Observable.just(new PartialResult<IDataSet<Pair<T, S>>>(1.0, retval));
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        // Callables provide lazy evaluation
        Observable<PartialResult<R>> prz = this.zero(() -> sketch.zero());
        Callable<R> c = () -> sketch.create(this.data);
        Observable<R> o = Observable.fromCallable(c);
        Observable<PartialResult<R>> pro = o.map(e -> new PartialResult<R>(e));
        Observable<PartialResult<R>> result = prz.concatWith(pro);
        if (this.separateThread)
            result = result.observeOn(Schedulers.computation());
        return result;
    }

    @Override
    public String toString() {
        return "LocalDataSet " + this.data.toString();
    }
}
