package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.*;

import java.util.ArrayList;

public class ParallelDataset<T> implements IDataset<T> {
    protected ArrayList<IDataset<T>> children;

    protected static class ParallelReporter<S> implements IResultReporter<S> {
        protected class IndexedReporter<S> implements IResultReporter<S> {
            double done;
            int    index;

            IndexedReporter(int index) {
                this.index = index;
                this.done = 0;
            }

            @Override
            public void report(double done, S data) {

            }

            @Override
            public void reportComplete(S data) {

            }

            @Override
            public void reportException(Exception ex) {

            }
        }

        IResultReporter<S> reporter;
        ArrayList<IndexedReporter<S>> reporters;

        public ParallelReporter(int count, IResultReporter<S> reporter) {
            this.reporter = reporter;
            this.reporters = new ArrayList<IndexedReporter<S>>(count);
            for (int i=0; i < count; i++)
                this.reporters.add(new IndexedReporter<S>(i));
        }

        @Override
        public void report(double done, S data) {
            // TODO
        }

        @Override
        public void reportComplete(S data) {
            // TODO
        }

        @Override
        public void reportException(Exception ex) {
            // TODO
        }
    }

    public ParallelDataset(ArrayList<IDataset<T>> children) {
        this.children = children;
    }

    @Override
    public <S> void map(IMap<T, S> mapper,
                        IResultReporter<IDataset<S>> result,
                        CancellationToken ct) {
        // TODO
    }

    @Override
    public <S> void zip(IDataset<S> other,
                        IResultReporter<IDataset<Pair<T, S>>> result,
                        CancellationToken ct) {
        // TODO
    }

    @Override
    public <R> void sketch(Sketch<T, R> sketch,
                           IResultReporter<R> result,
                           CancellationToken ct) {
        // TODO
    }
}
