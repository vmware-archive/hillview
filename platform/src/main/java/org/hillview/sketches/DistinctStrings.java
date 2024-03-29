/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.hillview.dataset.api.IJsonSketchResult;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * A set that contains strings.
 */
public class DistinctStrings implements IJsonSketchResult {
    static final long serialVersionUID = 1;
    private final ObjectOpenHashSet<String> uniqueStrings;

    DistinctStrings() {
        this.uniqueStrings = new ObjectOpenHashSet<String>();
    }

    private DistinctStrings(ObjectOpenHashSet<String> hash) {
        this.uniqueStrings = hash;
    }

    void add(@Nullable String string) {
        this.uniqueStrings.add(string);
    }

    public int size() {
        return this.uniqueStrings.size();
    }

    /**
     * @return the union of two sets. The maxSize is the larger of the two. If one
     * of them allow for unbounded size (maxSize = 0) then so does the union.
     */
    DistinctStrings union(final DistinctStrings otherSet) {
        ObjectOpenHashSet<String> hash = new ObjectOpenHashSet<String>(
                Math.max(this.uniqueStrings.size(), otherSet.uniqueStrings.size()));
        hash.addAll(this.uniqueStrings);
        hash.addAll(otherSet.uniqueStrings);
        return new DistinctStrings(hash);
    }

    public Iterable<String> getStrings() {
        return this.uniqueStrings;
    }

    public String toString() {
        return this.uniqueStrings.toString();
    }

    public String[] getQuantiles(int nQuantiles) {
        String[] data = this.uniqueStrings.toArray(new String[0]);
        Arrays.sort(data);
        if (data.length <= nQuantiles)
            return data;
        String[] result = new String[nQuantiles];
        for (int i = 0; i < nQuantiles; i++)
            result[i] = data[i * data.length / nQuantiles];
        return result;
    }

    public void addAll(Iterable<String> data) {
        for (String s: data)
            this.add(s);
    }
}
