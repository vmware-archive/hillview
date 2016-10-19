package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.*;

public class LocalDataset<T> implements IDataset<T> {
    private T data;
    // TODO: run on a separate thread

    private static class ProgressAdaptor<T> implements IProgressReporter {
        IResultReporter<T> resultReporter;

        ProgressAdaptor(IResultReporter<T> resultReporter) {
            this.resultReporter = resultReporter;
        }

        @Override
        public void report(double done) {
            resultReporter.report(done, null);
        }
    }

    public LocalDataset(T data) {
        this.data = data;
    }

    @Override
    public <S> void map(IMap<T, S> mapper,
                        IResultReporter<IDataset<S>> result,
                        CancellationToken ct) {
        try {
            ProgressAdaptor<IDataset<S>> adaptor = new ProgressAdaptor<IDataset<S>>(result);
            S r = mapper.map(data, adaptor, ct);
            LocalDataset<S> retval = new LocalDataset<S>(r);
            result.reportComplete(retval);
        } catch (Exception ex) {
            result.reportException(ex);
        }
    }

    @Override
    public <S> void zip(IDataset<S> other,
                        IResultReporter<IDataset<Pair<T, S>>> result,
                        CancellationToken ct) {
        try {
            if (!(other instanceof LocalDataset<?>))
                throw new RuntimeException("Unexpected type in Zip " + other);
            LocalDataset<S> lds = (LocalDataset<S>) other;
            Pair<T, S> data = new Pair<T, S>(this.data, lds.data);
            LocalDataset<Pair<T, S>> retval = new LocalDataset<Pair<T, S>>(data);
            result.reportComplete(retval);
        } catch (Exception ex) {
            result.reportException(ex);
        }
    }

    @Override
    public <R> void sketch(Sketch<T, R> sketch,
                           IResultReporter<R> result,
                           CancellationToken ct) {
        try {
            ProgressAdaptor<R> adaptor = new ProgressAdaptor<R>(result);
            R r = sketch.create(this.data, adaptor, ct);
            result.reportComplete(r);
        } catch (Exception ex) {
            result.reportException(ex);
        }
    }
}
