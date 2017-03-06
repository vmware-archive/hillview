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
 *
 */

package org.hiero.sketch;

import org.hiero.utils.Randomness;
import org.junit.Test;
import java.util.Arrays;

public class PercentileTest {
    @Test
    public void plotPercentile() {
        final int resolution = 2000;
        final int invErrSq= 100;
        final int sampleSize = 5*invErrSq*resolution;
        final int range = 1000000;
        int i;
        int j;
        int runs;

        for (runs = 0; runs < 1; runs++) {
            final int[] percentile = new int[resolution];
            final Randomness rn = Randomness.getInstance();
            for (i = 0; i < sampleSize; i++) {
                j = (int) Math.floor((rn.nextInt(range) * resolution) / range);
                percentile[j]++;
            }
            /*
            for (j = 0; j < 100; j++)
            System.out.printf("Bucket: %d, Count: %d%n", j, percentile[j]);
            */
            Arrays.sort(percentile);
            /*
            System.out.printf("Min: %d, Mean: %d, Max: %d%n",
                    percentile[0], sampleSize / resolution, percentile[resolution -1]);
            */
        }
    }
}

