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

package org.hillview.utils;

import org.hillview.dataset.api.IJsonSketchResult;

/**
 * A count and confidence around the counted value.
 */
public class CountWithConfidence implements IJsonSketchResult {
    static final long serialVersionUID = 1;

    public final long count;
    public final long confidence;

    public CountWithConfidence(long count, long confidence) {
        this.count = count;
        this.confidence = confidence;
    }

    public CountWithConfidence(long count) {
        this(count, 0);
    }

    public CountWithConfidence add(Noise noise) {
        return new CountWithConfidence(
                this.count + Converters.toLong(noise.getNoise()),
                this.confidence + Converters.toLong(noise.get2Stdev()));
                /* TODO: this is not a real CI. Not sure how this is being used currently */
    }
}
