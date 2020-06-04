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

package org.hillview.sketches.highorder;

import org.hillview.dataset.api.IncrementalTableSketch;
import org.hillview.dataset.api.IScalable;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.table.QuantizationSchema;
import org.hillview.table.QuantizedTable;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * A QuantizedTableSketch runs another sketch (childSketch) over a quantized table.
 * @param <R>    Result produced by child and this sketch.
 * @param <S>    Type of child sketch.
 * @param <SW>   Type of workspace used by the child sketch.
 */
public class QuantizedTableSketch<
        R extends ISketchResult & IScalable<R>,
        S extends IncrementalTableSketch<R, SW>,
        SW extends ISketchWorkspace> extends IncrementalTableSketch<R, SW> {
    private final S childSketch;
    private final QuantizationSchema quantizationSchema;

    public QuantizedTableSketch(S childSketch, QuantizationSchema qs) {
        this.childSketch = childSketch;
        this.quantizationSchema = qs;
    }

    @Override
    public void add(SW workspace, R result, int rowNumber) {
        this.childSketch.add(workspace, result, rowNumber);
    }

    @Override
    public SW initialize(ITable data) {
        ITable qt = new QuantizedTable(data, this.quantizationSchema);
        return this.childSketch.initialize(qt);
    }

    @Nullable
    @Override
    public R zero() {
        return this.childSketch.zero();
    }

    @Nullable
    @Override
    public R add(@Nullable R left, @Nullable R right) {
        return this.childSketch.add(left, right);
    }
}
