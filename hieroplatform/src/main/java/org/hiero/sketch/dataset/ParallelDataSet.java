package org.hiero.sketch.dataset;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.*;
import rx.Observable;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelDataSet<T> implements IDataSet<T> {
    int bundleInterval = 0;  // If non zero then add up partial results that come close to each other
    static final TimeUnit bundleTimeUnit = TimeUnit.MILLISECONDS;

    @NonNull
    private final ArrayList<IDataSet<T>> children;
    protected static Logger logger = Logger.getLogger(ParallelDataSet.class.getName());

    private ParallelDataSet(@NonNull final Map<Integer, IDataSet<T>> elements) {
        this.children = new ArrayList<IDataSet<T>>(elements.size());
        for (final Map.Entry<Integer, IDataSet<T>> e : elements.entrySet())
            this.children.add(e.getKey(), e.getValue());
    }

    public ParallelDataSet(@NonNull final ArrayList<IDataSet<T>> children) {
        this.children = children;
    }

    private int size() { return this.children.size(); }

    public void setBundleInterval(int timeIntervalInMilliseconds) {
        this.bundleInterval = timeIntervalInMilliseconds;
        if (timeIntervalInMilliseconds < 0)
            throw new RuntimeException("Negative time interval: " + timeIntervalInMilliseconds);
    }

    private <S> S log(S data, String message) {
        logger.log(Level.INFO, message);
        return data;
    }

    // This function groups R values that come too close in time (within a 'bundleInterval'
    // time interval) and "adds" them up emitting a single value.
    public <R> Observable<R> bundle(final Observable<R> data, IMonoid<R> adder) {
        if (this.bundleInterval > 0)
            return data.buffer(this.bundleInterval, bundleTimeUnit)
                       .filter(e -> !e.isEmpty())  // we don't want lots of zeros
                       .map(l -> adder.reduce(l));
        else
            return data;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(@NonNull final IMap<T, S> mapper) {
        ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>>(this.size());
        for (int i = 0; i < this.size(); i++) {
            int finalI = i;
            Observable<Pair<Integer, PartialResult<IDataSet<S>>>> ci =
                    this.children.get(i)
                            .map(mapper)
                            .map(e -> new Pair<Integer, PartialResult<IDataSet<S>>>(finalI, e));
            obs.add(i, ci);
        }
        Observable<Pair<Integer, PartialResult<IDataSet<S>>>> merged =
                Observable.merge(obs);

        Observable<PartialResult<IDataSet<S>>> map = merged.filter(p -> p.second.deltaValue != null)
                .toMap(p -> p.first, p -> p.second.deltaValue)
                .single()
                .map(m -> new PartialResult<IDataSet<S>>(0.0, new ParallelDataSet<S>(m)));
        Observable<PartialResult<IDataSet<S>>> dones =
                merged.map(p -> p.second.deltaDone / this.size())
                        .map(e -> new PartialResult<IDataSet<S>>(e, null));
        Observable<PartialResult<IDataSet<S>>> result = dones.concatWith(map);
        result = bundle(result, new PartialResultMonoid<IDataSet<S>>(new OptionMonoid<IDataSet<S>>()));
        return result;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        if (!(other instanceof ParallelDataSet<?>))
            throw new RuntimeException("Expected a ParallelDataSet " + other);
        final ParallelDataSet<S> os = (ParallelDataSet<S>)other;
        final int mySize = this.size();
        if (mySize != os.size())
            throw new RuntimeException("Different sizes for ParallelDatasets: " +
                    mySize + " vs. " + os.size());
        final ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>>>();
        for (int i = 0; i < mySize; i++) {
            final IDataSet<S> oChild = os.children.get(i);
            final IDataSet<T> tChild = this.children.get(i);
            final Observable<PartialResult<IDataSet<Pair<T, S>>>> zip = tChild.zip(oChild).last();
            final int finalI = i;
            obs.add(zip.map(
                    e -> new Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>(finalI, e)));
        }
        final Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>> merged =
                Observable.merge(obs);

        final Observable<PartialResult<IDataSet<Pair<T, S>>>> result =
                merged.filter(p -> p.second.deltaValue != null)
                .toMap(p -> p.first, p -> p.second.deltaValue)
                .single()
                .map(m -> new PartialResult<IDataSet<Pair<T, S>>>(
                        0.0, new ParallelDataSet<Pair<T, S>>(m)));
        final Observable<PartialResult<IDataSet<Pair<T, S>>>> dones =
                merged.map(p -> p.second.deltaDone / this.size())
                        .map(e -> new PartialResult<IDataSet<Pair<T, S>>>(e, null));
        return dones.concatWith(result);
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        ArrayList<Observable<PartialResult<R>>> obs = new ArrayList<Observable<PartialResult<R>>>();
        int mySize = this.size();
        for (int i = 0; i < mySize; i++) {
            IDataSet<T> child = this.children.get(i);
            Observable<PartialResult<R>> sk =
                    child.sketch(sketch)
                    .map(e -> log(e, "child sketch done"))
                    .map(e -> new PartialResult<R>(e.deltaDone / mySize, e.deltaValue));
            obs.add(sk);
        }
        Observable<PartialResult<R>> result = Observable.merge(obs);
        result = result.map(e -> log(e, "child merge done"));
        result = bundle(result, prm);
        return result;
    }

    @Override
    public String toString() {
        return "ParallelDataSet of size " + this.size();
    }
}
