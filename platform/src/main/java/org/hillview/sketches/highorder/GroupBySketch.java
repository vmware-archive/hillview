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

package org.hillview.sketches.highorder;

import org.hillview.dataset.api.IncrementalTableSketch;
import org.hillview.dataset.api.IScalable;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.sketches.results.Groups;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;

/**
 * Given a TableSketch S, this applies S to each group.
 * The groups are defined by buckets, given by IHistogramBuckets.
 * @param <R>  Result computed by the basic sketch S.
 * @param <S>  The type of a a table sketch.
 */
public class GroupBySketch<
        R extends ISketchResult & IScalable<R>,
        SW extends ISketchWorkspace,
        S extends IncrementalTableSketch<R, SW>>
        extends IncrementalTableSketch<Groups<R>, GroupByWorkspace<SW>> {
    protected final S missingSketch;
    protected final JsonList<S> bucketSketch;
    protected final IHistogramBuckets buckets;

    protected GroupBySketch(IHistogramBuckets buckets,
                            S sketch) {
        this.missingSketch = sketch;
        this.bucketSketch = new JsonList<S>(buckets.getBucketCount());
        for (int i = 0; i < buckets.getBucketCount(); i++)
            this.bucketSketch.add(sketch);
        this.buckets = buckets;
    }

    @Override
    public void add(GroupByWorkspace<SW> workspace, Groups<R> result, int rowNumber) {
        if (workspace.column.isMissing(rowNumber)) {
            this.missingSketch.add(workspace.missingWorkspace, result.perMissing, rowNumber);
        } else {
            if (workspace.endColumn != null) {
                // Interval columns: contribute to all buckets that overlap the interval
                int index0 = this.buckets.indexOf(workspace.column, rowNumber);
                int index1 = this.buckets.indexOf(workspace.endColumn, rowNumber);
                if (index0 > index1) {
                    int tmp = index0;
                    index0 = index1;
                    index1 = tmp;
                }
                int max = Math.min(index1, result.perBucket.size() - 1);
                for (int index = Math.max(index0, 0); index <= max; index++) {
                    this.bucketSketch.get(index).add(
                            workspace.bucketWorkspace.get(index), result.perBucket.get(index), rowNumber);
                }
            } else {
                int index = this.buckets.indexOf(workspace.column, rowNumber);
                if (index >= 0 && index < result.perBucket.size())
                    this.bucketSketch.get(index).add(workspace.bucketWorkspace.get(index), result.perBucket.get(index), rowNumber);
            }
        }
    }

    @Override
    public GroupByWorkspace<SW> initialize(ITable data) {
        IColumn column = Converters.checkNull(data).getLoadedColumn(this.buckets.getColumn());
        SW missing = this.missingSketch.initialize(data);
        JsonList<SW> bucketWorkspaces = Linq.map(this.bucketSketch, s -> s.initialize(data));
        return new GroupByWorkspace<SW>(column, bucketWorkspaces, missing);
    }

    @Override
    public Groups<R> zero() {
        int b = this.buckets.getBucketCount();
        JsonList<R> perBucket = new JsonList<R>(b);
        for (int i = 0; i < b; i++)
            perBucket.add(this.bucketSketch.get(i).zero());
        R perMissing = this.missingSketch.zero();
        return new Groups<R>(perBucket, Converters.checkNull(perMissing));
    }

    @Nullable
    @Override
    public Groups<R> add(@Nullable Groups<R> left, @Nullable Groups<R> right) {
        if (Converters.checkNull(left).perBucket.size() != Converters.checkNull(right).perBucket.size())
            throw new RuntimeException("Incompatible sizes for groups: " +
                    left.perBucket.size() + " and " + right.perBucket.size());
        R perMissing = this.missingSketch.add(left.perMissing, right.perMissing);
        JsonList<R> perBucket = Linq.zipMap(left.perBucket, right.perBucket,
                Linq.map(this.bucketSketch, s -> s::add));
        return new Groups<R>(perBucket, Converters.checkNull(perMissing));
    }
}
