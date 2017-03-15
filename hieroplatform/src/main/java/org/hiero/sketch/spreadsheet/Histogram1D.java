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

package org.hiero.sketch.spreadsheet;

import com.google.gson.JsonElement;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

import javax.annotation.Nullable;

/**
 * One Dimensional histogram. Does not contain the column and membershipMap
 */
public class Histogram1D extends BaseHist1D implements IJson {
    private final Bucket1D[] buckets;
    private long missingData;
    private long outOfRange;

    public Histogram1D(final IBucketsDescription1D bucketDescription) {
        super(bucketDescription);
        this.buckets = new Bucket1D[bucketDescription.getNumOfBuckets()];
        for (int i = 0; i < this.bucketDescription.getNumOfBuckets(); i++)
            this.buckets[i] = new Bucket1D();
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    @Override
    public void createHistogram(final IColumn column, final IMembershipSet membershipSet,
                                @Nullable final IStringConverter converter) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (column.isMissing(currRow))
                this.missingData++;
            else {
                double val = column.asDouble(currRow, converter);
                int index = this.bucketDescription.indexOf(val);
                if (index >= 0)
                    this.buckets[index].add(val, column.getObject(currRow));
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public void addItem(final double value, @Nullable final Object item) {
        int index = this.bucketDescription.indexOf(value);
        if (index >= 0)
            this.buckets[index].add(value,item);
        else this.outOfRange++;
    }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's bucket or exception if doesn't exist.
     */
    public Bucket1D getBucket(final int index) { return this.buckets[index]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram1D union( Histogram1D otherHistogram) {
        if (!this.bucketDescription.equals(otherHistogram.bucketDescription))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        Histogram1D unionH = new Histogram1D(this.bucketDescription);
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++)
            unionH.buckets[i] = this.buckets[i].union(otherHistogram.buckets[i]);
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }
}