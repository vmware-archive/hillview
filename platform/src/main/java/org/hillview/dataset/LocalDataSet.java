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
 */

package org.hillview.dataset;

import org.hillview.dataset.api.*;
import org.hillview.utils.Converters;
import org.hillview.utils.ExecutorUtils;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Pair;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * A LocalDataSet is an implementation of IDataSet which contains exactly one
 * item of type T.
 * @param <T> type of data held in the dataset.
 */
public class LocalDataSet<T> extends BaseDataSet<T> {
    /**
     * Actual data held by the LocalDataSet.
     * This should be private, but we make it public for ease of testing.
     */
    @Nullable
    public final T data;
    /**
     * If this is set to 'true' then data processing (i.e., the map and test calls)
     * are done on a separate thread.  This is the only place where multithreading
     * is used in the whole platform code base.  The effect is that all observers of
     * the results are invoked on a separate thread.
     */
    private final boolean separateThread;

    /**
     * Work is executed on this thread.
     */
    private static final Scheduler workScheduler;

    static {
        ExecutorService executor = ExecutorUtils.getComputeExecutorService();
        workScheduler = Schedulers.from(executor);
    }

    /**
     * Create a LocalDataSet, processing the data on a separate thread by default.
     * @param data: Data to store in the LocalDataSet.
     */
    public LocalDataSet(@Nullable final T data) {
        this.data = data;
        this.separateThread = true;
    }

    public LocalDataSet(@Nullable final T data, final boolean separateThread) {
        this.data = data;
        this.separateThread = separateThread;
    }

    /**
     * Schedule the computation using the LocalDataSet.workScheduler.
     * @param data  Data whose computation is scheduled
     */
    private <S> Observable<S> schedule(Observable<S> data) {
        if (this.separateThread) {
            return data.subscribeOn(LocalDataSet.workScheduler)
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler());
        }
        return data;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        // Actual map computation performed lazily when observable is subscribed to.
        final Callable<IDataSet<S>> callable = () -> {
            try {
                HillviewLogger.instance.info("Starting map", "{0}:{1}",
                        this, mapper.asString());
                S result = mapper.apply(LocalDataSet.this.data);
                HillviewLogger.instance.info("Completed map", "{0}:{1}",
                        this, mapper.asString());
                return new LocalDataSet<S>(result);
            } catch (final Throwable t) {
                throw new Exception(t);
            }
        };
        final Observable<IDataSet<S>> mapped = Observable.fromCallable(callable);
        // Wrap the produced data in a PartialResult
        Observable<PartialResult<IDataSet<S>>> data = mapped
                .map(PartialResult::new);
        return this.schedule(data);
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper) {
        // Actual map computation performed lazily when observable is subscribed to.
        final Callable<IDataSet<S>> callable = () -> {
            try {
                List<S> list = mapper.apply(LocalDataSet.this.data);
                List<IDataSet<S>> locals = new ArrayList<IDataSet<S>>();
                for (S s : Converters.checkNull(list)) {
                    HillviewLogger.instance.info("Starting flatMap", "{0}:{1}",
                            this, mapper.asString());
                    IDataSet<S> ds = new LocalDataSet<S>(s);
                    HillviewLogger.instance.info("Completed flatMap", "{0}:{1}",
                            this, mapper.asString());
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
        return this.schedule(data);
    }

    @Override
    public <S, R> Observable<PartialResult<IDataSet<R>>> zip(
            IDataSet<S> other, IMap<Pair<T, S>, R> map) {
        if (!(other instanceof LocalDataSet<?>))
            throw new RuntimeException("Unexpected type in Zip " + other);
        LocalDataSet<S> lds = (LocalDataSet<S>) other;
        Pair<T, S> data = new Pair<T, S>(this.data, lds.data);
        LocalDataSet<R> retval = new LocalDataSet<R>(map.apply(data));
        return Observable.just(new PartialResult<IDataSet<R>>(retval));
    }

    @Override
    public <R> Observable<PartialResult<IDataSet<R>>> zipN(List<IDataSet<T>> other, IMap<List<T>, R> map) {
        List<T> data = new ArrayList<T>();
        data.add(this.data);
        for (IDataSet<T> d: other) {
            if (!(d instanceof LocalDataSet<?>))
                throw new RuntimeException("Unexpected type in ZipN " + other);
            LocalDataSet<T> lds = (LocalDataSet<T>)d;
            data.add(lds.data);
        }
        LocalDataSet<R> retval = new LocalDataSet<R>(map.apply(data));
        return Observable.just(new PartialResult<IDataSet<R>>(retval));
    }

    @Override
    public Observable<PartialResult<IDataSet<T>>> prune(IMap<T, Boolean> isEmpty) {
        final Callable<IDataSet<T>> callable = () -> {
            HillviewLogger.instance.info("Starting prune", "{0}:{1}",
                    this, isEmpty.asString());
            Boolean result = isEmpty.apply(LocalDataSet.this.data);
            HillviewLogger.instance.info("Completed prune", "{0}:{1} result is {2}",
                    this, isEmpty.asString(), result);
            assert result != null;
            return result ? null : this;
        };
        final Observable<IDataSet<T>> result = Observable.fromCallable(callable);
        // Wrap the produced data in a PartialResult
        Observable<PartialResult<IDataSet<T>>> data = result
                .map(PartialResult::new);
        return this.schedule(data);
    }

    @Override
    public Observable<PartialResult<ControlMessage.StatusList>> manage(ControlMessage message) {
        final Callable<ControlMessage.StatusList> callable = () -> {
            HillviewLogger.instance.info("Starting manage", "{0}:{1}",
                    this, message.toString());
            ControlMessage.Status status;
            try {
                status = message.localAction(this);
            } catch (final Throwable t) {
                status = new ControlMessage.Status("Exception", t);
                HillviewLogger.instance.error("Exception during manage", t);
            }
            ControlMessage.StatusList result = new ControlMessage.StatusList(status);
            HillviewLogger.instance.info("Completed manage", "{0}:{1}",
                    this, message.toString());
            return result;
        };
        final Observable<ControlMessage.StatusList> executed = Observable.fromCallable(callable);
        return executed.map(PartialResult::new);
    }

    @Override
    public <R extends ISketchResult> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        // Immediately return a zero partial result
        // final Observable<PartialResult<R>> zero = this.zero(sketch::zero);
        final Callable<R> callable = () -> {
            try {
                HillviewLogger.instance.info("Starting sketch", "{0}:{1}",
                        this, sketch.asString());
                R result = sketch.create(this.data);
                HillviewLogger.instance.info("Completed sketch", "{0}:{1}",
                        this, sketch.asString());
                return result;
            } catch (final Throwable t) {
                throw new Exception(t);
            }
        };
        final Observable<R> sketched = Observable.fromCallable(callable);
        // Wrap results in a stream of PartialResults.
        final Observable<PartialResult<R>> pro = sketched.map(PartialResult::new);
        // Concatenate with the zero.
        //Observable<PartialResult<R>> result = zero.concatWith(pro);
        return this.schedule(pro);
    }

    @Override
    public String toString() {
        return super.toString() + ":" + this.data;
    }
}
