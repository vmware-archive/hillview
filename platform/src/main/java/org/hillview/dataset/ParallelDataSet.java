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
import org.hillview.utils.HillviewLogger;
import rx.Observable;
import rx.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A ParallelDataSet holds together multiple IDataSet objects and invokes operations on them
 * concurrently.  It also implements IDataSet itself.
 * @param <T>  Type of data in the DataSet.
 */
public class ParallelDataSet<T> extends BaseDataSet<T> {
    /*
     * A ParallelDataSet invokes operations on children concurrently.  It then combines the
     * results obtained form its children into a single stream.  It also has the option to
     * aggregate results from children if they come "close" to each other in time, to produce
     * fewer results upstream.  This parameter controls the aggregation interval:
     * If non zero then aggregate partial results that are within
     * this specified number of milliseconds from each other.  Human reaction time is on the
     * order of 50 milliseconds or more, so this is a ballpark reasonable value.
     * If this is set to zero no aggregation is performed.
     * If this is set to a value too large then progress reporting to the user may be impacted.
     */
    private int bundleInterval = 250;
    /**
     * The bundleInterval specifies a time in milliseconds.
     */
    private static final TimeUnit bundleTimeUnit = TimeUnit.MILLISECONDS;
    /**
     * If this is set to true there is some additional logging inserted.
     */
    private static final boolean useLogging = false;

    /**
     * Children of the data set.
     */

    private final List<IDataSet<T>> children;

    /**
     * Create a ParallelDataSet from a map that indicates the index of each child.
     * @param elements  Children, presented as a map indexed by child position.
     */
    private ParallelDataSet(final Map<Integer, IDataSet<T>> elements) {
        this.children = new ArrayList<IDataSet<T>>(elements.size());
        for (final Map.Entry<Integer, IDataSet<T>> e : elements.entrySet())
            this.children.add(e.getKey(), e.getValue());
    }

    /**
     * Create a ParallelDataSet from a list of children.
     * @param children  List of children.
     */
    public ParallelDataSet(final List<IDataSet<T>> children) {
        this.children = children;
    }

    private int size() { return this.children.size(); }

    /**
     * Can be used to change the time interval in which partial results are aggregated.
     * This should be done only once after construction; datasets are supposed to be immutable.
     * @param timeIntervalInMilliseconds The datasets waits this much to receive results and
     *                                   aggregates them.  If 0 all results are sent immediately.
     */
    public void setBundleInterval(int timeIntervalInMilliseconds) {
        this.bundleInterval = timeIntervalInMilliseconds;
        if (timeIntervalInMilliseconds < 0)
            throw new RuntimeException("Negative time interval: " + timeIntervalInMilliseconds);
    }

    /**
     * This function groups R values that come too close in time (within a 'bundleInterval'
     * time interval) and "adds" them up emitting a single value.
     * @param data  A stream of data.
     * @param adder A monoid that knows how to add the data.
     * @return  A shorter stream, in which some of the values in the data stream have been
     * added together.
     */
    private <R> Observable<R> bundle(final Observable<R> data, IMonoid<R> adder) {
        if (this.bundleInterval > 0) {
            // If a time interval has no data we don't want to produce a zero.
            Observable<List<R>> bundled = data.buffer(this.bundleInterval, bundleTimeUnit)
                       .filter(e -> !e.isEmpty());
            if (ParallelDataSet.useLogging)
                bundled = bundled.map(e -> this.logPipe(e, "bundling " + e.size() + " values"));
            return bundled.map(adder::reduce);
        } else {
            return data;
        }
    }

    /**
     * Run a map computation over all children.
     * @param mapper  Computation to run on the dataset.
     * @param <S>     Type of result data.
     * @return        A stream of partial results produced by running the mapper on all children.
     */
    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(
             final IMap<T, S> mapper) {
        // Autoconnect does not propagate unsubscriptions so we
        // have to do it manually.  We save the subscription
        // here so we can disconnect it when necessary.
        Subscription savedSubscription[] = new Subscription[1];
        HillviewLogger.instance.info("Invoked map", "target={0}", this);
        final List<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>>(this.size());
        // We run the mapper over each child, and then we tag the results produced by
        // the child with the child index.
        for (int i = 0; i < this.size(); i++) {
            int finalI = i;
            final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> ci =
                    this.children.get(i)
                            .map(mapper)
                            .map(e -> new Pair<Integer, PartialResult<IDataSet<S>>>(finalI, e));
            obs.add(i, ci);
        }
        // Merge the streams from all children
        final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> merged =
                // publish().autoConnect(2) ensures that the two consumers
                // of this stream pull from the *same* stream, and not from
                // two different copies.
                Observable.merge(obs)
                        .publish()
                        .autoConnect(2, s -> savedSubscription[0] = s);
        // We split the merged stream of PartialResults into two separate streams
        // - mapResult for the actual PartialResult.deltaValue
        // - dones for the PartialResult.doneValue
        // The dones we "send" out immediately to indicate progress,
        // whereas the mapResult part we process locally
        final Observable<PartialResult<IDataSet<S>>> mapResult =
                // drop partial results which have no value
                merged.filter(p -> Converters.checkNull(p.second).deltaValue != null)
                      // Create a java.Util.Map with all the non-null results;
                      // there should be exactly one per child
                      .toMap(p -> p.first, p -> Converters.checkNull(p.second).deltaValue)
                      // We expect to produce a single map
                      .single()
                      // Finally, create a ParallelDataSet from the map; these have 0 'done' progress
                      .map(m -> new PartialResult<IDataSet<S>>(0.0, new ParallelDataSet<S>(m)));
        final Observable<PartialResult<IDataSet<S>>> dones =
                // Each child produces a 1/this.size() fraction of the result.
                merged.map(p -> Converters.checkNull(p.second).deltaDone / this.size())
                        .map(e -> new PartialResult<IDataSet<S>>(e, null));
        Observable<PartialResult<IDataSet<S>>> result = dones.mergeWith(mapResult);
        result = bundle(result, new PRDataSetMonoid<S>())
            .doOnUnsubscribe(() -> {
                if (savedSubscription[0] != null)
                    savedSubscription[0].unsubscribe();
            });
        return result;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper) {
        // Autoconnect does not propagate unsubscriptions so we
        // have to do it manually.  We save the subscription
        // here so we can disconnect it when necessary.
        Subscription savedSubscription[] = new Subscription[1];
        HillviewLogger.instance.info("Invoked flatMap", "target={0}", this);
        final List<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<S>>>>>(this.size());
        // We run the mapper over each child, and then we tag the results produced by
        // the child with the child index.
        for (int i = 0; i < this.size(); i++) {
            int finalI = i;
            final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> ci =
                    this.children.get(i)
                            .flatMap(mapper)
                            .map(e -> new Pair<Integer, PartialResult<IDataSet<S>>>(finalI, e));
            obs.add(i, ci);
        }

        if (obs.isEmpty())
            throw new RuntimeException("Empty children");
        // Merge the streams from all children
        final Observable<Pair<Integer, PartialResult<IDataSet<S>>>> merged =
                // publish().autoConnect(2) ensures that the two consumers
                // of this stream pull from the *same* stream, and not from
                // two different copies.
                Observable.merge(obs)
                        .publish()
                        .autoConnect(2, s -> savedSubscription[0] = s);
        // We split the merged stream of PartialResults into two separate streams
        // - mapResult for the actual PartialResult.deltaValue
        // - dones for the PartialResult.doneValue
        // The dones we "send" out immediately to indicate progress,
        // whereas the mapResult part we process locally
        final Observable<PartialResult<IDataSet<S>>> mapResult =
                // drop partial results which have no value
                merged.filter(p -> Converters.checkNull(p.second).deltaValue != null)
                        // Create a java.Util.Map with all the non-null results;
                        // there should be exactly one per child
                        .toMap(p -> p.first, p -> Converters.checkNull(p.second).deltaValue)
                        // We expect to produce a single map
                        .single()
                        // Finally, create a ParallelDataSet from the map; these have 0 'done' progress
                        .map(m -> new PartialResult<IDataSet<S>>(0.0, new ParallelDataSet<S>(m)));
        final Observable<PartialResult<IDataSet<S>>> dones =
                // Each child produces a 1/this.size() fraction of the result.
                merged.map(p -> Converters.checkNull(p.second).deltaDone / this.size())
                        .map(e -> new PartialResult<IDataSet<S>>(e, null));
        Observable<PartialResult<IDataSet<S>>> result = dones.mergeWith(mapResult);
        result = bundle(result, new PRDataSetMonoid<S>())
                .doOnUnsubscribe(() -> {
                    if (savedSubscription[0] != null)
                        savedSubscription[0].unsubscribe();
                });
        return result;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(
            final IDataSet<S> other) {
        // Autoconnect does not propagate unsubscriptions so we
        // have to do it manually.  We save the subscription
        // here so we can disconnect it when necessary.
        Subscription savedSubscription[] = new Subscription[1];

        HillviewLogger.instance.info("Invoked zip", "target={0}", this);
        if (!(other instanceof ParallelDataSet<?>))
            throw new RuntimeException("Expected a ParallelDataSet " + other);
        final ParallelDataSet<S> os = (ParallelDataSet<S>)other;
        final int mySize = this.size();
        if (mySize != os.size())
            throw new RuntimeException("Different sizes for ParallelDatasets: " +
                    mySize + " vs. " + os.size());
        final List<Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>>> obs =
                new ArrayList<Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>>>();
        // Just zip children pairwise; tag each result with the child index
        for (int i = 0; i < mySize; i++) {
            final IDataSet<S> oChild = os.children.get(i);
            final IDataSet<T> tChild = this.children.get(i);
            final Observable<PartialResult<IDataSet<Pair<T, S>>>> zip = tChild.zip(oChild).last();
            final int finalI = i;
            obs.add(zip.map(
                    e -> new Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>(finalI, e)));
        }
        final Observable<Pair<Integer, PartialResult<IDataSet<Pair<T, S>>>>> merged =
                // publish().autoConnect(2) ensures that the two consumers
                // of this stream pull from the *same* stream, and not from
                // two different copies.
                Observable.merge(obs)
                        .publish()
                        .autoConnect(2, s -> savedSubscription[0] = s);
        // We split the merged stream of PartialResults into two separate streams
        // - zipResult for the actual PartialResult.deltaValue
        // - dones for the PartialResult.doneValue
        // The dones we "send" out immediately to indicate progress,
        // whereas the zipResult part we process locally.
        final Observable<PartialResult<IDataSet<Pair<T, S>>>> zipResult =
                merged.filter(p -> Converters.checkNull(p.second).deltaValue != null)
                      // Convert to a java.utils.Map
                      .toMap(p -> p.first, p -> Converters.checkNull(p.second).deltaValue)
                      .single()
                      .map(m -> new PartialResult<IDataSet<Pair<T, S>>>(
                            0.0, new ParallelDataSet<Pair<T, S>>(m)));
        final Observable<PartialResult<IDataSet<Pair<T, S>>>> dones =
                // Each child produces a 1/this.size() fraction of the result.
                merged.map(p -> Converters.checkNull(p.second).deltaDone / this.size())
                      .map(e -> new PartialResult<IDataSet<Pair<T, S>>>(e, null));
        Observable<PartialResult<IDataSet<Pair<T, S>>>> result = dones.mergeWith(zipResult);
        PRDataSetMonoid<Pair<T, S>> prm = new PRDataSetMonoid<Pair<T, S>>();
        result = bundle(result, prm)
                .doOnUnsubscribe(() -> {
                    if (savedSubscription[0] != null)
                        savedSubscription[0].unsubscribe();
                });
        return result;
    }

    @Override
    public Observable<PartialResult<ControlMessage.StatusList>> manage(ControlMessage message) {
        HillviewLogger.instance.info("Invoked manage", "target={0}", this);
        List<Observable<PartialResult<ControlMessage.StatusList>>> obs =
                new ArrayList<Observable<PartialResult<ControlMessage.StatusList>>>();
        final int mySize = this.size();
        // Run over each child separately
        for (int i = 0; i < mySize; i++) {
            IDataSet<T> child = this.children.get(i);
            Observable<PartialResult<ControlMessage.StatusList>> sk = child.manage(message);
            sk = sk.map(e -> new PartialResult<ControlMessage.StatusList>(
                    e.deltaDone / mySize, e.deltaValue));
            obs.add(sk);
        }

        final Callable<ControlMessage.StatusList> callable = () -> {
            HillviewLogger.instance.info("Starting manage", "{0}", message.toString());
            ControlMessage.Status status;
            try {
                status = message.parallelAction(this);
            } catch (final Throwable t) {
                status = new ControlMessage.Status("Exception", t);
            }
            ControlMessage.StatusList result = new ControlMessage.StatusList();
            if (status != null)
                result.add(status);
            HillviewLogger.instance.info("Completed manage", "{0}", message.toString());
            return result;
        };
        final Observable<ControlMessage.StatusList> executed = Observable.fromCallable(callable);
        final Observable<PartialResult<ControlMessage.StatusList>> pr = executed.map
                (l -> new PartialResult<ControlMessage.StatusList>(0, l));
        obs.add(pr);

        Observable<PartialResult<ControlMessage.StatusList>> merged = Observable.merge(obs);
        merged = this.bundle(merged, new PartialResultMonoid<ControlMessage.StatusList>(new
                ControlMessage.StatusListMonoid()));
        return merged;
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        HillviewLogger.instance.info("Invoked sketch", "target={0}", this);
        List<Observable<PartialResult<R>>> obs = new ArrayList<Observable<PartialResult<R>>>();
        final int mySize = this.size();
        // Run sketch over each child separately
        for (int i = 0; i < mySize; i++) {
            IDataSet<T> child = this.children.get(i);
            final int finalI = i;
            Observable<PartialResult<R>> sk = child.sketch(sketch);
            if (useLogging)
                sk = sk.map(e -> this.logPipe(e, "child " + finalI + " sketch result " + sketch));
            sk = sk.map(e -> new PartialResult<R>(e.deltaDone / mySize, e.deltaValue));
            obs.add(sk);
        }
        // Just merge all sketch results
        Observable<PartialResult<R>> result = Observable.merge(obs);
        if (useLogging)
            result = result.map(e -> this.logPipe(e, "after merge " + sketch.toString()));
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        result = this.bundle(result, prm)
            .doOnUnsubscribe(
                    () -> HillviewLogger.instance.info("Sketch unsubscribe", "{0}:{1}",
                            this, sketch));
        if (useLogging)
            result = result.map(e -> this.logPipe(e, "after bundle " + sketch.toString()));
        return result;
    }

    @Override
    public String toString() {
        return super.toString() + ", size " + this.size();
    }
}
