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

package org.hillview.sketches;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.Schema;

import java.io.Serializable;

public class CountSketchDescription implements Serializable {
    /**
     * The number of buckets that CountSketch will hash items into.
     */
    public int buckets;
    /**
     * The number of independent repetitions.
     */
    public int trials;
    /**
     * The hash functions used: one independent hash function per repetition.
     */
    public LongHashFunction[] hashFunction;
    /**
     * The schema for the tuples over which the sketch is being run.
     */
    public Schema schema;

    public CountSketchDescription(int buckets, int trials, long seed, Schema schema) {
        this.buckets = buckets;
        this.trials = trials;
        LongHashFunction hash = LongHashFunction.xx(seed);
        this.hashFunction = new LongHashFunction[trials];
        for (int i = 0; i < trials; i++)
            this.hashFunction[i] = LongHashFunction.xx(hash.hashInt(i));
        this.schema = schema;
    }
}
