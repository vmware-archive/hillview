/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.hillview.utils.JsonList;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
/**
 *  A data structure that computes the minHash of a column of a table.
 *  It stores the k column values
 *  that hash to the minimum value.
 */
public class MinKSet<T> extends BucketsInfo {
    static final long serialVersionUID = 1;
    
    public final Comparator<T> comp;
    public final int maxSize;
    public final Long2ObjectRBTreeMap<T> data;
    @Nullable public T min;
    @Nullable public T max;

    public MinKSet(int maxSize, Comparator<T> comp) {
        this.maxSize = maxSize;
        this.comp = comp;
        this.data = new Long2ObjectRBTreeMap<T>();
        this.min = null;
        this.max = null;
        this.presentCount = 0;
        this.missingCount = 0;
    }

    public MinKSet(int maxSize, Long2ObjectRBTreeMap<T> data, Comparator<T> comp,
            @Nullable T min, @Nullable T max, long numPresent, long numMissing) {
        this.comp = comp;
        this.maxSize = maxSize;
        this.data = data;
        this.min = min;
        this.max = max;
        this.presentCount = numPresent;
        this.missingCount = numMissing;
    }

    public List<T> getSamples() {
        List<T> samples = new ArrayList<T>(this.data.values());
        samples.sort(this.comp);
        return samples;
    }

    public int size() {
        return this.data.size();
    }

    /**
     * Returns true if we have fewer or equal strings to the number of buckets.
     * @param buckets Number of buckets we want.
     */
    public boolean allStringsKnown(int buckets) {
        if (this.min == null)
            // no non-null values
            return true;
        return this.data.size() <= buckets;
    }

    /**
     * This method will return (at most) a prescribed number of left bucket boundaries.
     * @param maxBuckets The maximum number of buckets.
     * @return An ordered list of boundaries for b <= maxBuckets buckets. If the number of distinct
     * strings is small, the number of buckets b could be strictly smaller than maxBuckets.
     * If the number of buckets is b, the number of boundaries is also b. The first bucket starts at
     * min.  The buckets boundaries are all distinct, hence the number
     * of buckets returned might be smaller.
     */
    public JsonList<T> getLeftBoundaries(int maxBuckets) {
        if (this.min == null) {
            // No non-null values
            JsonList <T> boundaries = new JsonList<T>(1);
            boundaries.add(null);
            return boundaries;
        }
        List<T> samples = this.getSamples();
        if (!samples.contains(this.min))
            samples.add(0, this.min);
        JsonList <T> boundaries = new JsonList<T>(maxBuckets);
        Utilities.equiSpaced(samples, maxBuckets, boundaries);
        return boundaries;
    }
}
