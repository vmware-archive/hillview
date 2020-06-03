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

package org.hillview.sketches;

import org.hillview.table.api.ISketchWorkspace;
import org.hillview.utils.Randomness;

/**
 * Implements reservoir sampling.
 */
public class ReservoirSampleWorkspace implements ISketchWorkspace {
    /**
     * Number of samples desired.
     */
    private final int samplesDesired;
    private int currentIndex;
    /**
     * Random number generator used during construction only.
     */
    private final Randomness random;
    /**
     * Insert a row only when skipRows is 0.
     */
    private int skipRows;
    private double w;

    public ReservoirSampleWorkspace(int samplesDesired, long seed) {
        if (samplesDesired <= 0)
            throw new RuntimeException("Illegal number of samples " + samplesDesired);
        this.samplesDesired = samplesDesired;
        this.random = new Randomness(seed);
        this.currentIndex = 0;
        this.skipRows = 0;
        this.w = Math.exp(Math.log(this.random.nextDouble()) / this.samplesDesired);
    }

    /**
     * Returns the index of this sample in the result array, or -1 if the sample is to be discarded.
     */
    public int sampleIndex() {
        if (this.samplesDesired > this.currentIndex) {
            return this.currentIndex++;
        }
        this.currentIndex++;
        boolean sample = this.skipRows == 0;
        if (sample) {
            this.skipRows = this.random.nextGeometric(this.w);
            this.w = this.w * Math.exp(Math.log(this.random.nextDouble() / this.samplesDesired));
            return this.random.nextInt(this.samplesDesired);
        }
        this.skipRows--;
        return -1;
    }
}
