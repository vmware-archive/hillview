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

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.dataStructures.*;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.DoubleDataRangeSketch;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.columns.IntColumnPrivacyMetadata;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.utils.JsonList;

import java.sql.SQLException;
import java.util.function.BiFunction;

public class PrivateSimpleDBTarget extends SimpleDBTarget {
    private final DPWrapper wrapper;

    PrivateSimpleDBTarget(JdbcConnectionInformation conn, HillviewComputation c,
                                 PrivacySchema privacySchema) {
        super(conn, c);
        this.wrapper = new DPWrapper(privacySchema);
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        this.runCompleteSketch(
                this.table, this.makeSketch(summary, new SummarySketch()),
                (d, c) -> this.wrapper.addPrivateMetadata(d), request, context);
    }

    @Override
    public String toString() {
        return "Private local database: " + this.jdbc.toString();
    }

    @HillviewRpc
    public void getDataRanges1D(RpcRequest request, RpcRequestContext context) {
        RangeArgs[] args = request.parseArgs(RangeArgs[].class);
        assert args.length == 1;
        double min, max;

        ColumnPrivacyMetadata md = this.wrapper.privacySchema.get(args[0].cd.name);
        RangeFilterDescription filter = this.wrapper.columnLimits.get(args[0].cd.name);
        if (md instanceof DoubleColumnPrivacyMetadata) {
            DoubleColumnPrivacyMetadata dmd = (DoubleColumnPrivacyMetadata)md;
            if (filter == null) {
                min = dmd.globalMin;
                max = dmd.globalMax;
            } else {
                min = dmd.roundDown(filter.min);
                max = dmd.roundUp(filter.max);
            }
        } else if (md instanceof IntColumnPrivacyMetadata) {
            IntColumnPrivacyMetadata imd = (IntColumnPrivacyMetadata)md;
            if (filter == null) {
                min = imd.globalMin;
                max = imd.globalMax;
            } else {
                min = imd.roundDown((int)filter.min);
                max = imd.roundUp((int)filter.max);
            }
        } else {
            throw new RuntimeException("Not yet implemented");
        }

        DataRange retRange = new DataRange(min, max);
        retRange.presentCount = -1;
        retRange.missingCount = -1;
        PrecomputedSketch<ITable, DataRange> sk =
                new PrecomputedSketch<ITable, DataRange>(retRange, new DoubleDataRangeSketch(args[0].cd.name));
        BiFunction<DataRange, HillviewComputation, JsonList<BucketsInfo>> post = (e, c) -> {
            JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
            result.add(e);
            return result;
        };
        this.runCompleteSketch(this.table, sk, post, request, context);
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        DPWrapper.PrivateHistogramArgs[] info = request.parseArgs(DPWrapper.PrivateHistogramArgs[].class);
        ColumnPrivacyMetadata metadata = this.wrapper.privacySchema.get(info[0].cd.name);
        double epsilon = metadata.epsilon;
        assert info.length == 2;

        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        IDyadicDecomposition dd = info[0].getDecomposition(metadata);
        IDyadicDecomposition cdd = info[1].getDecomposition(metadata);
        try {
            this.database.connect();
            Histogram histo = this.database.histogram(cd, dd.getHistogramBuckets());
            Histogram cdf = this.database.histogram(cd, cdd.getHistogramBuckets());
            Pair<AugmentedHistogram, HistogramPrefixSum> result = new
                    Pair<AugmentedHistogram, HistogramPrefixSum>(
                    new AugmentedHistogram(histo), new HistogramPrefixSum(cdf));
            this.database.disconnect();
            ISketch<ITable, Pair<AugmentedHistogram, HistogramPrefixSum>> sk = this.makeSketch(result);
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
