package org.hiero.sketch.dataset.api;

/**
 * A distributed dataset with elements of type T in the leaves.
 *
 * @param <T> Type of elements stored in the dataset.
 */
public interface IDataset<T> {
    <S> void map(IMap<T, S> mapper,
             IResultReporter<IDataset<S>> result,
             CancellationToken ct);

    <S> void zip(IDataset<S> other,
             IResultReporter<IDataset<Pair<T, S>>> result,
             CancellationToken ct);

    <R> void sketch(Sketch<T, R> sketch,
                IResultReporter<R> result,
                CancellationToken ct);
}
