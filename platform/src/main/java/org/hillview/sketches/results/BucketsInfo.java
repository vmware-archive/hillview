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

import java.io.Serializable;

/**
 * This interface is a base class for all sketch results that are
 * used to compute the buckets of a histogram, i.e., various forms of quantiles.
 */
public abstract class BucketsInfo implements Serializable, IJson {
    public long presentCount;
    public long missingCount;

    protected BucketsInfo() {
        this.presentCount = 0;
        this.missingCount = 0;
    }

    public void addMissing() {
        this.missingCount++;
    }
}
