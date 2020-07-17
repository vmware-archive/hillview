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

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.IScalable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;

/**
 * Represents a count (e.g., of elements a histogram bucket); if the count is 1
 * also keeps information about the unique element.
 */
public class CountAndSingleton implements IJsonSketchResult, IScalable<CountAndSingleton> {
    public long count;
    @Nullable
    public RowSnapshot row;

    public CountAndSingleton() {
        this(0, null);
    }

    public CountAndSingleton(long l, @Nullable RowSnapshot row) {
        this.count = l;
        this.row = row;
    }

    public void increment(VirtualRowSnapshot row, int rowIndex) {
        this.count++;
        if (this.count == 1) {
            row.setRow(rowIndex);
            this.row = row.materialize();
        } else {
            this.row = null;
        }
    }

    @Override
    public String toString() {
        return this.count +
                (this.row != null ? this.row.toString() : "");
    }

    public CountAndSingleton add(CountAndSingleton other) {
        long count = this.count + other.count;
        RowSnapshot row = null;
        if (count == 1)
            row = this.row != null ? this.row : other.row;
        return new CountAndSingleton(count, row);
    }

    @Override
    public CountAndSingleton rescale(double samplingRate) {
        if (samplingRate >= 1.0)
            return this;
        throw new RuntimeException("Cannot be rescaled");
    }

    public Count getCount() {
        return new Count(this.count);
    }
}
