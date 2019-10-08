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

import org.hillview.dataset.api.IJson;

public class DataRange extends BucketsInfo implements IJson {
    public double min;
    public double max;

    public DataRange() {
        this.min = 0;
        this.max = 0;
    }

    public DataRange(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public void add(double val) {
        if (this.presentCount == 0) {
            this.min = val;
            this.max = val;
        } else if (val < this.min) {
            this.min = val;
        } else if (val > this.max) {
            this.max = val;
        }
        this.presentCount++;
    }

    public DataRange add(DataRange right) {
        DataRange result = new DataRange();
        result.presentCount = this.presentCount + right.presentCount;
        result.missingCount = this.missingCount + right.missingCount;
        if (right.presentCount == 0) {
            result.min = this.min;
            result.max = this.max;
        } else if (this.presentCount == 0) {
            result.min = right.min;
            result.max = right.max;
        } else {
            result.min = Math.min(this.min, right.min);
            result.max = Math.max(this.max, right.max);
        }
        return result;
    }
}
