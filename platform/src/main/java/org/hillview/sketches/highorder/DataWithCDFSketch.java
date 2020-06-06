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

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.Groups;
import org.hillview.utils.Pair;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * This post-processed sketch integrates the second argument as post-processing.
 * @param <D>  Type of first argument.
 */
public class DataWithCDFSketch<D> extends
        PostProcessedSketch<ITable, Pair<D, Groups<Count>>, Pair<D, Groups<Count>>> {
    public DataWithCDFSketch(ISketch<ITable, Pair<D, Groups<Count>>> sketch) {
        super(sketch);
    }

    @Nullable
    @Override
    public Pair<D, Groups<Count>> postProcess(@Nullable Pair<D, Groups<Count>> result) {
        D first = Converters.checkNull(Converters.checkNull(result).first);
        Groups<Count> cdf = Converters.checkNull(result.second);
        return new Pair<>(first, cdf.prefixSum(Count::add, Groups::new));
    }
}
