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

package org.hillview.dataStructures;

public class Noise {
    public double noise;
    public double variance;
    private double confidence;

    public Noise() {
        this.noise = 0;
        this.variance = 0;
        this.confidence = 0;
    }

    public double getConfidence() {
        if (this.confidence <= 0)
            this.confidence = 2 * Math.sqrt(this.variance);
        return this.confidence;
    }

    public void clear() {
        this.noise = 0;
        this.variance = 0;
        this.confidence = 0;
    }
}
