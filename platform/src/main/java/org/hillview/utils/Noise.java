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

package org.hillview.utils;

public class Noise {
    private double noise;
    private double variance;

    public Noise() {
        this.clear();
    }

    public Noise(double noise, double variance) {
        this.set(noise, variance);
    }

    public double getConfidence() {
        return 2 * Math.sqrt(this.variance);
    }

    public void clear() {
        this.set(0, 0);
    }

    public void add(double noise, double variance) {
        this.noise += noise;
        this.variance += variance;
    }

    public double getNoise() {
        return this.noise;
    }


    public void set(double noise, double variance) {
        this.noise = noise;
        this.variance = variance;
    }
}
