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

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * A partial result always describes a DELTA.  All streaming computations
 * return streams of partial results; adding these partial results together produces
 * the final result.
 * @param <T>  Type of data in partial result.
 */
public class PartialResult<T> implements Serializable {
    /**
     * How much more has been done from the computation
     * since the previous partial result.  A number between 0 and 1.  Even if this
     * is 1, it does not necessarily mean that the work is finished; this is necessary
     * due to rounding issues with double values.
     */
    public final double deltaDone;
    /**
     * Additional data computed since the previous partial result.
     */
    @Nullable
    public final T deltaValue;

    /**
     * Creates a partial result.
     * @param deltaDone  How much more has been done.  A number between 0 and 1.
     * @param deltaValue Extra result produced.
     */
    public PartialResult(double deltaDone, @Nullable T deltaValue) {
        if (deltaDone < 0) {
            throw new RuntimeException("Illegal value for deltaDone");
        } else if (deltaDone > 1) {
            // This can happen due to double addition imprecision.
            deltaDone = 1.0;
        }
        this.deltaDone = deltaDone;
        this.deltaValue = deltaValue;
    }

    /**
     * Creates a complete partial result (deltaDone = 1.0).
     * @param deltaValue Result produced.
     */
    public PartialResult(@Nullable T deltaValue) {
        this(1.0, deltaValue);
    }

    @Override
    public String toString() {
        return "PR[" + Double.toString(this.deltaDone) + "," + this.deltaValue + "]";
    }
}
