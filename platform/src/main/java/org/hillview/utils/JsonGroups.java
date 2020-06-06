/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.sketches.results.Count;

import java.util.function.Function;

/**
 * This is a version of Group which is serializable as JSON.
 * @param <R>  Type of data that is grouped.
 */
public class JsonGroups<R extends IJsonSketchResult> implements IJsonSketchResult, IGroup<R> {
    /**
     * For each bucket one result.
     */
    public final JsonList<R> perBucket;
    /**
     * For the bucket corresponding to the 'missing' value on result.
     */
    public final R           perMissing;

    /**
     * Create groups using a function generator.
     * @param size          Size of the groups.
     * @param constructor   Function that generates contents.  Invoked with index, -1 for missing.
     */
    public JsonGroups(int size, Function<Integer, R> constructor) {
        this.perMissing = constructor.apply(-1);
        this.perBucket = new JsonList<R>(size);
        for (int i = 0; i < size; i++)
            this.perBucket.add(constructor.apply(i));
    }

    /**
     * Create groups filled with a specific value.
     * @param size    Size of the group.
     * @param value   Value of contents.
     */
    public JsonGroups(int size, R value) {
        this.perMissing = value;
        this.perBucket = new JsonList<R>(size);
        for (int i = 0; i < size; i++)
            this.perBucket.add(value);
    }

    public JsonGroups(JsonList<R> perBucket, R perMissing) {
        this.perBucket = perBucket;
        this.perMissing = perMissing;
    }

    @Override
    public R getMissing() {
        return this.perMissing;
    }

    public int size() {
        return this.perBucket.size();
    }

    @Override
    public R getBucket(int index) {
        if (index < 0)
            return this.perMissing;
        else
            return this.perBucket.get(index);
    }

    public static JsonGroups<Count> fromArray(long[] data, long missing) {
        return new JsonGroups<Count>(data.length, index -> index < 0 ? new Count(missing) : new Count(data[index]));
    }

    public static JsonGroups<Count> fromArray(long[] data) {
        return fromArray(data, 0);
    }

    public static JsonGroups<JsonGroups<Count>> fromArray(long[][] data) {
        return new JsonGroups<JsonGroups<Count>>(data.length,
                index -> index < 0 ?
                        new JsonGroups<Count>(data[0].length, new Count(0)):
                        fromArray(data[index]));
    }

    public static JsonGroups<Count> fromArray(int[] data, int missing) {
        return new JsonGroups<Count>(data.length, index -> index < 0 ? new Count(missing) : new Count(data[index]));
    }

    public static JsonGroups<Count> fromArray(int[] data) {
        return fromArray(data, 0);
    }

    public static JsonGroups<JsonGroups<Count>> fromArray(int[][] data) {
        return new JsonGroups<JsonGroups<Count>>(data.length,
                index -> index < 0 ?
                        new JsonGroups<Count>(data[0].length, new Count(0)):
                        fromArray(data[index]));
    }
}
