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

package org.hiero.sketches;

import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IStringConverter;

import javax.annotation.Nullable;

/**
 * Abstract class for a one dimensional histogram. Derived classes mainly vary in the way they
 * implement buckets.
 */

public abstract class BaseHist1D implements IHistogram1D {
    final protected IBucketsDescription1D bucketDescription;

    public BaseHist1D(final IBucketsDescription1D bucketDescription) {
        this.bucketDescription = bucketDescription;
    }

    @Override
    public int getNumOfBuckets() { return this.bucketDescription.getNumOfBuckets(); }

    @Override
    public void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                                      @Nullable final IStringConverter converter, double sampleRate) {
        this.createHistogram(column, membershipSet.sample(sampleRate), converter);
    }

    @Override
    public void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                                      @Nullable final IStringConverter converter, double sampleRate, long seed) {
        this.createHistogram(column, membershipSet.sample(sampleRate, seed), converter);
    }
}
