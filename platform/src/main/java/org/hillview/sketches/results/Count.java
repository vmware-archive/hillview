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

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.hillview.dataset.api.IJsonSketchResult;

import java.util.Objects;

/**
 * Represents a count (e.g., of elements a histogram bucket).
 */
public class Count implements IJsonSketchResult {
    public long count;

    public Count() {
        this(0);
    }

    public Count(long l) {
        this.count = l;
    }

    public void add(long l) {
        this.count += l;
    }

    @Override
    public String toString() {
        return Long.toString(this.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Count count1 = (Count) o;
        return count == count1.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public JsonElement toJsonTree() { return new JsonPrimitive(this.count); }
}
