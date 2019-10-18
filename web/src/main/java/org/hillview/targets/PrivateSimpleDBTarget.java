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

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.results.Heatmap;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.utils.Converters;

import java.sql.SQLException;

public class PrivateSimpleDBTarget extends SimpleDBTarget implements IPrivateDataset {
    private final DPWrapper wrapper;

    PrivateSimpleDBTarget(JdbcConnectionInformation conn, HillviewComputation c,
                                 PrivacySchema privacySchema) {
        super(conn, c);
        this.wrapper = new DPWrapper(privacySchema);
    }

    private PrivateSimpleDBTarget(PrivateSimpleDBTarget other, HillviewComputation computation) {
        super(other.jdbc, computation);
        this.wrapper = new DPWrapper(other.wrapper);
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
        this.wrapper.getDataQuantiles1D(request, context, this);
    }

    @HillviewRpc
    public void getDataQuantiles2D(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles2D(request, context, this);
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;

        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        ColumnQuantization quantization = this.wrapper.privacySchema.quantization(cd.name);
        double epsilon = this.wrapper.privacySchema.epsilon(cd.name);
        if (quantization == null)
            throw new RuntimeException("No quantization information for column " + cd.name);

        DyadicDecomposition d0 = info[0].getDecomposition(quantization);
        DyadicDecomposition d1 = info[1].getDecomposition(quantization);
        try {
            this.database.connect();
            Histogram histo = this.database.histogram(cd, info[0].getBuckets(quantization), quantization);
            Histogram cdf = this.database.histogram(cd, info[1].getBuckets(quantization), quantization);
            Pair<Histogram, Histogram> result = new Pair<Histogram, Histogram>(histo, cdf);
            this.database.disconnect();
            ISketch<ITable, Pair<Histogram, Histogram>> sk = new PrecomputedSketch<ITable, Pair<Histogram, Histogram>>(result);
            this.runCompleteSketch(this.table, sk, (e, c) ->
                    new Pair<PrivateHistogram, PrivateHistogram>(
                            new PrivateHistogram(d0, Converters.checkNull(e.first), epsilon, false),
                            new PrivateHistogram(d1, Converters.checkNull(e.second), epsilon, true)),
                    request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        this.wrapper.filterRange(request, context, this,
                (d, c) -> new PrivateSimpleDBTarget((PrivateSimpleDBTarget)d, c));
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        this.wrapper.filter2DRange(request, context, this,
                (d, c) -> new PrivateSimpleDBTarget((PrivateSimpleDBTarget)d, c));
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        try {
            this.database.connect();
            int result = this.database.distinctCount(col.columnName);
            this.database.disconnect();
            DistinctCount dc = new DistinctCount(result);
            ISketch<ITable, DistinctCount> sk = new PrecomputedSketch<ITable, DistinctCount>(dc);
            // TODO(pratiksha): add noise to this count
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        try {
            this.database.connect();
            Heatmap heatmap = this.database.heatmap(
                    info[0].cd, info[1].cd,
                    info[0].getBuckets(), info[1].getBuckets(),
                    null, null);
            this.database.disconnect();
            double epsilon = this.wrapper.privacySchema.epsilon(new String[] {
                    info[0].cd.name, info[1].cd.name});
            ColumnQuantization q0 = this.wrapper.privacySchema.quantization(info[0].cd.name);
            ColumnQuantization q1 = this.wrapper.privacySchema.quantization(info[1].cd.name);
            Converters.checkNull(q0);
            Converters.checkNull(q1);
            DyadicDecomposition d0 = info[0].getDecomposition(q0);
            DyadicDecomposition d1 = info[1].getDecomposition(q1);
            PrivateHeatmap result = new PrivateHeatmap(d0, d1, heatmap, epsilon);
            ISketch<ITable, Heatmap> sk = new PrecomputedSketch<ITable, Heatmap>(result.heatmap);
            this.runCompleteSketch(this.table, sk, (e, c) -> e, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IDataSet<ITable> getDataset() {
        return this.table;
    }

    @Override
    public DPWrapper getWrapper() {
        return this.wrapper;
    }
}
