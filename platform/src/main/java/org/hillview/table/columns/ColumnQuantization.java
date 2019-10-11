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

package org.hillview.table.columns;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This class is used to describe how values used in a column are quantized
 * when performing computations.  The quantization is used when computing
 * differentially-private views over data synopses.  Quantization involves
 * placing the value into one of a finite number of sorted buckets.
 */
public class ColumnQuantization implements IJson, Serializable {
    /**
     * Quantize a numeric value by rounding down.
     * @param value  Value to quantize.
     * @return       One of the quantization interval boundaries.
     *               Throws on values smaller than the minimum.
     */
    public double roundDown(double value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Quantize a string value by rounding down.
     * @param value  Value to quantize.
     * @return       One of the quantization interval boundaries.
     *               Throws on values smaller than the minimum.
     */
    @Nullable
    public String roundDown(@Nullable String value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Check if a value is too large or too small.
     * @param value  Value to check
     * @return       True if the value is bigger than the range maximum or smaller than
     *               the range minimum.
     */
    public boolean outOfRange(double value)  {
        throw new UnsupportedOperationException();
    }

    /**
     * Check if a value is too large or too small.
     * @param value  Value to check
     * @return       True if the value is bigger than the range maximum or smaller than
     *               the range minimum.
     */
    public boolean outOfRange(@Nullable String value)  {
        throw new UnsupportedOperationException();
    }
}
