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

package org.hillview.sketches;

import javax.annotation.Nullable;

public class Bucket2D {
    private final Bucket1D bucket1;
    private final Bucket1D bucket2;
    private long count;

    public Bucket2D() {
        this.bucket1 = new Bucket1D();
        this.bucket2 = new Bucket1D();
        this.count = 0;
    }

    private Bucket2D(final long count, final Bucket1D bucket1, final Bucket1D bucket2) {
        this.count = count;
        this.bucket1 = bucket1;
        this.bucket2 = bucket2;
    }

    @Nullable
    public Object getMinObject1() { return this.bucket1.getMinObject(); }

    @Nullable
    public Object getMinObject2() { return this.bucket2.getMinObject(); }

    @Nullable
    public Object getMaxObject1() { return this.bucket1.getMaxObject(); }

    @Nullable
    public Object getMaxObject2() { return this.bucket2.getMaxObject(); }

    public double getMinValue1() { return this.bucket1.getMinValue(); }

    public double getMinValue2() { return this.bucket2.getMinValue(); }

    public double getMaxValue1() { return this.bucket1.getMaxValue(); }

    public double getMaxValue2() { return this.bucket2.getMaxValue(); }

    public long getCount() { return this.count; }

    public void add(final double val1, @Nullable final Object currObject1,
                    final double val2, @Nullable final Object currObject2) {
        this.bucket1.add(val1, currObject1);
        this.bucket2.add(val2, currObject2);
        this.count++;
    }

    public boolean isEmpty() { return this.count == 0; }

    /**
     * @return A bucket with the union count of the two buckets and the min/max updated accordingly. Procedure allows
     * both buckets to have objects of different types.
     */
    public Bucket2D union(final Bucket2D otherBucket) {
        long ucount = this.count + otherBucket.count;
        Bucket1D uBucket1 = this.bucket1.union(otherBucket.bucket1);
        Bucket1D uBucket2 = this.bucket2.union(otherBucket.bucket2);
        return new Bucket2D(ucount, uBucket1, uBucket2);
    }
}
