package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.*;
import rx.Observable;

import java.util.ArrayList;
import java.util.Map;

public class ParallelDataSet<T> implements IDataSet<T> {
    private final ArrayList<IDataSet<T>> children;

    private ParallelDataSet(final Map<Integer, IDataSet<T>> elements) {
        this.children = new ArrayList<IDataSet<T>>(elements.size());
        for (final Map.Entry<Integer, IDataSet<T>> e : elements.entrySet())
            this.children.add(e.getKey(), e.getValue());
    }

    public ParallelDataSet(final ArrayList<IDataSet<T>> children) {
        this.children = children;
    }

    private int size() { return this.children.size(); }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        final ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>>(this.size());
        for (int i = 0; i < this.size(); i++) {
            final int finalI = i;
            final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> ci =
                    this.children.get(i)
                            .map(mapper)
                            .map(e -> new Pair<Integer, PartialResult<IDataSet<S>>>(finalI, e));
            obs.add(i, ci);
        }
        final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> merged =
                Observable.merge(obs);

        final Observable<PartialResult<IDataSet<S>>> map = merged.filter(p -> p.second.deltaValue != null)
                .toMap(p -> p.first, p -> p.second.deltaValue)
                .single()
                .map(m -> new PartialResult<IDataSet<S>>(0.0, new ParallelDataSet<S>(m)));
        final Observable<PartialResult<IDataSet<S>>> dones =
                merged.map(p -> p.second.deltaDone / this.size())
                        .map(e -> new PartialResult<IDataSet<S>>(e, null));
        return dones.concatWith(map);
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
        final ArrayList<Observable<PartialResult<R>>> obs = new ArrayList<Observable<PartialResult<R>>>();
        final int mySize = this.size();
        for (int i = 0; i < mySize; i++) {
            final IDataSet<T> child = this.children.get(i);
            final Observable<PartialResult<R>> sk =
                    child.sketch(sketch)
                    .map(e -> new PartialResult<R>(e.deltaDone / this.size(), e.deltaValue));
            obs.add(sk);
        }
        return Observable.merge(obs);
    }

    @Override
    public String toString() {
        return "ParallelDataSet of size " + this.size();
    }
}
