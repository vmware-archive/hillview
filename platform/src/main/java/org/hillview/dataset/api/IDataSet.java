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

package org.hillview.dataset.api;

import org.hillview.dataset.PRDataSetMonoid;
import org.hillview.dataset.PostProcessedSketch;
import rx.Observable;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A distributed dataset with elements of type T in the leaves.
 * All operations on datasets return observables, which are streams of partial results.
 * Cancellation of an operation is done by unsubscribing from such an observable.
 *
 * @param <T> Type of elements stored in the dataset.
 */
public interface IDataSet<T> {
    /**
     * Run a computation on the dataset, return another dataset.
     * @param mapper  Computation to run on the dataset.
     * @param <S>     Type of result in the result dataset.
     * @return        A stream of partial results (all IDataSet[S]), only one of which should really
     *                be the actual result.  All the other ones should be null.
     */
    <S> Observable<PartialResult<IDataSet<S>>> map(IMap<T, S> mapper);

    /**
     * Run a computation on the dataset, return another dataset.
     * @param mapper  Computation to run on the dataset.
     * @param <S>     Type of result in the result dataset.
     * @return        A stream of partial results (all IDataSet[S]), only one of which should really
     *                be the actual result.  All the other ones should be null.
     */
    <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper);

    /**
     * Run a sketch on a dataset, returning a value.
     * @param sketch  Sketch computation to run on the dataset.
     * @param <R>     Type of result produced.
     * @return        A stream of partial results, all of type R.  Adding these partial results
     *                will produce the correct final result.  The sketch itself has an 'add' method.
     */
    <R extends ISketchResult> Observable<PartialResult<R>> sketch(ISketch<T, R> sketch);

    /**
     * Combine two datasets that have the exact same topology by pairing the values in the
     * corresponding leaves.
     * @param other  Dataset to combine with this.
     * @param <S>    Type of data in the other dataset.
     * @return       A stream of partial results which are all IDataSet[Pair[T,S]].  In fact this
     *               stream will contain exactly one result.
     */
    <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(IDataSet<S> other);

    /**
     * Remove from a dataset all leaves that are empty according to the supplied function.
     * (Also removes parallel datasets that have no children.)
     * @param isEmpty  Function that indicates when a local datasets holds an empty value.
     * @return         A stream of partial results which are IDataSet[T], which will contain
     *                 exactly one result.
     */
    Observable<PartialResult<IDataSet<T>>> prune(IMap<T, Boolean> isEmpty);

    /**
     * Execute the indicated control message at all layers of the IDataSet object.
     * @param message  Message to execute.
     * @return         A stream of partial results containing the status of all operations executed.
     */
    Observable<PartialResult<ControlMessage.StatusList>> manage(ControlMessage message);

    // The following are various helper methods.

    /**
     * Extracts the values from a stream of PartialResults.
     * @param results  Stream of partial results.
     * @param <R>      Type of data in partial results.
     * @return         A stream containing just the data of the partial results.
     */
    static <R> Observable<R> getValues(
            final Observable<PartialResult<R>> results) {
        return results.map(e -> e.deltaValue);
    }

    /**
     * Reduce a stream of values from a monoid.
     * @param data     Stream of values.
     * @param monoid   Monoid that knows how to add values.
     * @param <R>      Type of data to be reduced.
     * @return         A stream containing exactly one value - the sum of all
     *                 values in the data stream.
     */
    static <R> Observable<R> reduce(
            final Observable<R> data,
            final IMonoid<R> monoid) {
        return data.reduce(monoid.zero(), monoid::add);
    }

    static <S> Observable<IDataSet<S>> reduce(
            final Observable<PartialResult<IDataSet<S>>> results) {
        final IMonoid<PartialResult<IDataSet<S>>> mono = new PRDataSetMonoid<S>();
        return getValues(reduce(results, mono));
    }

    // The convenience methods below should be used only for testing.

    /**
     * Applies a map and then reduces the result immediately.
     * @param mapper  Mapper to apply to data.
     * @param <S>     Type of data in the result.
     * @return        An observable with a single element.
     */
    default <S> Observable<IDataSet<S>> singleMap(
            final IMap<T, S> mapper) {
        return reduce(this.map(mapper));
    }

    /**
     * Applies a prune and then reduces the result immediately.
     * @param mapper  Mapper to apply to data.
     * @return        An observable with a single element.
     */
    default Observable<IDataSet<T>> singlePrune(
            final IMap<T, Boolean> mapper) {
        return reduce(this.prune(mapper));
    }

    /**
     * Applies a flatMap and then reduces the result immediately.
     * @param mapper  Mapper to apply to data.
     * @param <S>     Type of data in the result.
     * @return        An observable with a single element.
     */
    default <S> Observable<IDataSet<S>> singleFlatMap(
            final IMap<T, List<S>> mapper) {
        return reduce(this.flatMap(mapper));
    }

    /**
     * Applies a zip and then reduces the result immediately.
     * @param <S>     Type of data in the result.
     * @return        An observable with a single element.
     */
    default <S> Observable<IDataSet<Pair<T, S>>> singleZip(
            final IDataSet<S> other) {
        return reduce(this.zip(other));
    }

    /**
     * Applies a sketch and then reduces the result immediately.
     * @param sketch  Sketch to apply to the data.
     * @param <R>     Type of data in the result.
     * @return        An observable with a single element.
     */
    default <R extends ISketchResult> Observable<R> singleSketch(
            final ISketch<T, R> sketch) {
        return reduce(getValues(this.sketch(sketch)), sketch);
    }

    default Observable<ControlMessage.StatusList> singleManage(
            final ControlMessage message) {
        return reduce(getValues(this.manage(message)), new ControlMessage.StatusListMonoid());
    }

    /**
     * Run a map synchronously.
     * @param mapper  Mapper to run.
     * @param <S>     Type of data produced.
     * @return        An IDataSet containing the final result of the map.
     */
    default <S> IDataSet<S> blockingMap(final IMap<T, S> mapper) {
        return this.singleMap(mapper).toBlocking().single();
    }

    /**
     * Run a flapMap synchronously.
     * @param mapper  Mapper to run.
     * @param <S>     Type of data produced.
     * @return        An IDataSet containing the final result of the flatMap.
     */
    default <S> IDataSet<S> blockingFlatMap(final IMap<T, List<S>> mapper) {
        return this.singleFlatMap(mapper).toBlocking().single();
    }

    /**
     * Run a zip synchronously.
     * @param <S>     Type of data produced.
     * @return        An IDataSet containing the final result of the zip.
     */
    @SuppressWarnings("unused")
    default <S> IDataSet<Pair<T, S>> blockingZip(final IDataSet<S> other) {
        return this.singleZip(other).toBlocking().single();
    }

    /**
     * Run a prune synchronously.
     * @return        An IDataSet containing the final result of the prune.
     */
    @SuppressWarnings("unused")
    @Nullable
    default IDataSet<T> blockingPrune(IMap<T, Boolean> map) {
        return this.singlePrune(map).toBlocking().single();
    }

    /**
     * Run a sketch synchronously.
     * @param sketch  Sketch to run.
     * @param <R>     Type of data produced.
     * @return        The result produced by the sketch.
     */
    @Nullable
    default <R extends ISketchResult> R blockingSketch(final ISketch<T, R> sketch) {
        return this.singleSketch(sketch).toBlocking().single();
    }

    /**
     * Run a post-processed sketch synchronously.
     * @param sketch  Post-processed sketch to run.
     * @param <R>     Type of data produced by sketch.
     * @param <F>     Type of result produced by postprocessing.
     * @return        The result produced by the sketch.
     */
    @Nullable
    default <R extends ISketchResult, F extends IJson> F
    blockingPostProcessedSketch(final PostProcessedSketch<T, R, F> sketch) {
        return sketch.postProcess(this.singleSketch(sketch.sketch).toBlocking().single());
    }

    @Nullable
    @SuppressWarnings("UnusedReturnValue")
    default ControlMessage.StatusList blockingManage(final ControlMessage message) {
        return this.singleManage(message).toBlocking().single();
    }
}
