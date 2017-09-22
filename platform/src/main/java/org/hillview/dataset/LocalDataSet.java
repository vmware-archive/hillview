/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.dataset;

import org.hillview.dataset.api.*;
import org.hillview.utils.HillviewLogManager;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * A LocalDataSet is an implementation of IDataSet which contains exactly one
 * item of type T.
 * @param <T> type of data held in the dataset.
 */
public class LocalDataSet<T> implements IDataSet<T> {
    /**
     * Actual data held by the LocalDataSet.
     */
    private final T data;
    /**
     * If this is set to 'true' then data processing (i.e., the map and test calls)
     * are done on a separate thread.  This is the only place where multithreading
     * is used in the whole platform code base.  The effect is that all observers of
     * the results are invoked on a separate thread.
     */
    private final boolean separateThread;

    /**
     * Create a LocalDataSet, processing the data on a separate thread by default.
     * @param data: Data to store in the LocalDataSet.
     */
    public LocalDataSet(final T data) {
        this.data = data;
        this.separateThread = true;
    }

    public LocalDataSet(final T data, final boolean separateThread) {
        this.data = data;
        this.separateThread = separateThread;
    }

    /**
     * Helper function to create the first result in a stream of results.
     * This is used to immediately return a "zero" when processing start;
     * the zero value is then updated with additional increments as processing
     * proceeds.  The zero is useful because it percolates through the invocation
     * chain all the way to the GUI, where it updates the progress bar.  This makes
     * it clear that processing has started even if no other partial results appear for
     * a long time.
     * @param z   A callable which produces the zero value.
     * @param <R> Type of result produced.
     * @return    An observable stream which contains just the zero value, produced lazily.
     */
    private <R> Observable<PartialResult<R>> zero(Callable<R> z) {
        // The callable is used to produce the zero value lazily only when someone subscribes
        // to the observable.
        Observable<R> zero = Observable.fromCallable(z);
        return zero.map(e-> new PartialResult<R>(0.0, e));
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        // Actual map computation performed lazily when observable is subscribed to.
        final Callable<IDataSet<S>> callable = () -> {
            try {
                HillviewLogManager.instance.logger.log(Level.INFO, "Starting map operation");
                S result = mapper.apply(LocalDataSet.this.data);
                HillviewLogManager.instance.logger.log(Level.INFO, "Map operation completed");
                return new LocalDataSet<S>(result);
            } catch (final Throwable t) {
                throw new Exception(t);
            }
        };
        final Observable<IDataSet<S>> mapped = Observable.fromCallable(callable);
        // Wrap the produced data in a PartialResult
        Observable<PartialResult<IDataSet<S>>> data = mapped.map(PartialResult::new);
        if (this.separateThread)
            data = data.observeOn(Schedulers.computation());
        return data;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper) {
        // Actual map computation performed lazily when observable is subscribed to.
        final Callable<IDataSet<S>> callable = () -> {
            try {
                List<S> list = mapper.apply(LocalDataSet.this.data);
                List<IDataSet<S>> locals = new ArrayList<IDataSet<S>>();
                for (S s : list) {
                    HillviewLogManager.instance.logger.log(
                            Level.INFO, "Starting flatMap operation");
                    IDataSet<S> ds = new LocalDataSet<S>(s);
                    HillviewLogManager.instance.logger.log(
                            Level.INFO, "FlatMap operation completed");
                    locals.add(ds);
                }
                return (IDataSet<S>) new ParallelDataSet<S>(locals);
            } catch (final Throwable t) {
                throw new Exception(t);
            }
        };
        final Observable<IDataSet<S>> mapped = Observable.fromCallable(callable);
        // Wrap the produced data in a PartialResult
        Observable<PartialResult<IDataSet<S>>> data = mapped.map(PartialResult::new);
        if (this.separateThread)
            data = data.observeOn(Schedulers.computation());
        return data;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        if (!(other instanceof LocalDataSet<?>))
            throw new RuntimeException("Unexpected type in Zip " + other);
        final LocalDataSet<S> lds = (LocalDataSet<S>) other;
        final Pair<T, S> data = new Pair<T, S>(this.data, lds.data);
        final LocalDataSet<Pair<T, S>> retval = new LocalDataSet<Pair<T, S>>(data);
        // This is very fast, so there is no need to use a callable or to return a zero.
        return Observable.just(new PartialResult<IDataSet<Pair<T, S>>>(retval));
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        // Immediately return a zero partial result
        final Observable<PartialResult<R>> zero = this.zero(sketch::zero);
        // Actual test computation performed lazily when observable is subscribed to.
        final Callable<R> callable = () -> {
            try {
                HillviewLogManager.instance.logger.log(
                        Level.INFO, "Starting sketch operation");
                R result = sketch.create(this.data);
                HillviewLogManager.instance.logger.log(
                        Level.INFO, "Sketch operation completed");
                return result;
            } catch (final Throwable t) {
                throw new Exception(t);
            }
        };
        final Observable<R> sketched = Observable.fromCallable(callable);
        // Wrap test results in a stream of PartialResults.
        final Observable<PartialResult<R>> pro = sketched.map(PartialResult::new);
        // Concatenate with the zero.
        Observable<PartialResult<R>> result = zero.concatWith(pro);
        if (this.separateThread)
            result = result.observeOn(Schedulers.computation());
        return result;
    }

    @Override
    public String toString() {
        return "LocalDataSet " + this.data.toString();
    }
}
