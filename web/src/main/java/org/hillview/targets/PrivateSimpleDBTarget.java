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
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;

import java.sql.SQLException;

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
                this.table, new PrecomputedSketch<ITable, TableSummary>(summary),
                (d, c) -> this.wrapper.addPrivateMetadata(d), request, context);
    }

    @Override
    public String toString() {
        return "Private local database: " + this.jdbc.toString();
    }

    @HillviewRpc
    public void getDataQuantiles1D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 1;
        double min, max;

        ColumnQuantization md = this.wrapper.privacySchema.quantization(args[0].cd.name);
        RangeFilterDescription filter = this.wrapper.columnLimits.get(args[0].cd.name);
        if (md instanceof DoubleColumnQuantization) {
            DoubleColumnQuantization dmd = (DoubleColumnQuantization)md;
            if (filter == null) {
                min = dmd.globalMin;
                max = dmd.globalMax;
            } else {
                min = dmd.roundDown(filter.min);
                max = dmd.roundDown(filter.max);
            }
        } else {
            throw new RuntimeException("Not yet implemented");
        }

        DataRange retRange = new DataRange(min, max);
        // TODO: compute these too
        // TODO(pratiksha): add noise to these counts
        retRange.presentCount = -1;
        retRange.missingCount = -1;
        PrecomputedSketch<ITable, DataRange> sk = new PrecomputedSketch<ITable, DataRange>(retRange);
        this.runCompleteSketch(this.table, sk, (e, c) -> new JsonList<BucketsInfo>(e), request, context);
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;

        ColumnQuantization metadata = this.wrapper.privacySchema.quantization(info[0].cd.name);
        double epsilon = this.wrapper.privacySchema.epsilon(info[0].cd.name);
        if (metadata == null)
            throw new RuntimeException("No quantization information for column " + info[0].cd.name);

        /*
        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        IDyadicDecomposition dd = info[0].getDecomposition(metadata);
        IDyadicDecomposition cdd = info[1].getDecomposition(metadata);
        try {
            this.database.connect();
            Histogram histo = this.database.histogram(cd, dd.getHistogramBuckets());
            Histogram cdf = this.database.histogram(cd, cdd.getHistogramBuckets());
            Pair<Histogram, Histogram> result = new Pair<Histogram, Histogram>(histo, cdf);
            this.database.disconnect();
            ISketch<ITable, Pair<Histogram, Histogram>> sk = new PrecomputedSketch<ITable, Pair<Histogram, Histogram>>(result);
            this.runCompleteSketch(this.table, sk, (e, c) ->
                    new Pair<PrivateHistogram, PrivateHistogram>(
                            new PrivateHistogram(dd, Converters.checkNull(e.first), epsilon, false),
                            new PrivateHistogram(cdd, Converters.checkNull(e.second), epsilon, true)),
                    request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        TODO
         */
    }
}
