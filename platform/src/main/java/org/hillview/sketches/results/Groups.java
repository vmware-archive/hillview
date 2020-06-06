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

package org.hillview.sketches.results;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.IScalable;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.utils.IGroup;
import org.hillview.utils.JsonGroups;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Combine multiple sketch results into a "vector" of sketch results.
 * This is used to compute sketches that perform group-by using a set of
 * buckets.  This sketch result will have one result per bucket, plus
 * one result for "missing" data.
 * @param <R>  Type of sketch result that is grouped.
 */
public class Groups<R extends ISketchResult & IScalable<R>>
        implements ISketchResult, IScalable<Groups<R>>, IGroup<R> {
    /**
     * For each bucket one result.
     */
    public final JsonList<R> perBucket;
    /**
     * For the bucket corresponding to the 'missing' value on result.
     */
    public final R           perMissing;
    /**
     * For the values that do not fall in any bucket.
     * Not yet useful, so commented-out.
    public final R           outOfRange;
     */

    public Groups(JsonList<R> perBucket, R perMissing /*, R outOfRange */) {
        this.perBucket = perBucket;
        this.perMissing = perMissing;
        //this.outOfRange = outOfRange;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < this.perBucket.size(); i++) {
            R ri = this.perBucket.get(i);
            result.append(i);
            result.append(" => ");
            result.append(ri.toString());
            result.append(System.lineSeparator());
        }
        result.append("missing => ");
        result.append(this.perMissing.toString());
        /*
        result.append(System.lineSeparator());
        result.append("out of range => ");
        result.append(this.outOfRange.toString());
        result.append(System.lineSeparator());
        */
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Groups<?> groups = (Groups<?>) o;
        return this.perBucket.equals(groups.perBucket) &&
                this.perMissing.equals(groups.perMissing) /* &&
                this.outOfRange.equals(groups.outOfRange) */;
    }

    public Groups<R> rescale(double samplingRate) {
        return this.map(r -> r.rescale(samplingRate));
    }

    @Override
    public int hashCode() {
        return Objects.hash(perBucket, perMissing /*, outOfRange */);
    }

    public <S extends ISketchResult & IScalable<S>> Groups<S> map(Function<R, S> map) {
        S missing = map.apply(this.perMissing);
        JsonList<S> perBucket = Linq.map(this.perBucket, map);
        return new Groups<S>(perBucket, missing);
    }

    public <J extends IJsonSketchResult> JsonGroups<J> toSerializable(Function<R, J> map) {
        J missing = map.apply(this.perMissing);
        JsonList<J> perBucket = Linq.map(this.perBucket, map);
        return new JsonGroups<J>(perBucket, missing);
    }

    @Override
    public R getMissing() {
        return this.perMissing;
    }

    @Override
    public int size() {
        return this.perBucket.size();
    }

    @Override
    public R getBucket(int index) {
        return this.perBucket.get(index);
    }
}
