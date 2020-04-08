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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.ConcurrentPostprocessedSketch;
import org.hillview.dataset.PostProcessedSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.maps.IdMap;
import org.hillview.dataset.PrecomputedSketch;
import org.hillview.sketches.results.Heatmap;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.NextKList;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.filters.RangeFilterPairDescription;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.sql.SQLException;

public class PrivateSimpleDBTarget extends SimpleDBTarget implements IPrivateDataset {
    static final long serialVersionUID = 1;

    // This class inherits columnLimits from SimpleDBTarget and it has another one
    // in the wrapper.  We only use the one in the wrapper.
    private final DPWrapper wrapper;

    PrivateSimpleDBTarget(JdbcConnectionInformation conn, HillviewComputation c,
                          PrivacySchema privacySchema, String schemaFilename) throws SQLException {
        super(conn, c);
        this.wrapper = new DPWrapper(privacySchema, schemaFilename);
        this.database.connect();
    }

    private PrivateSimpleDBTarget(PrivateSimpleDBTarget other, HillviewComputation computation) throws SQLException {
        super(other.jdbc, computation);
        this.wrapper = new DPWrapper(other.wrapper);
        this.database.connect();
    }

    private PrivacySchema getPrivacySchema() {
        return this.wrapper.getPrivacySchema();
    }

    @HillviewRpc
    public void changePrivacy(RpcRequest request, RpcRequestContext context) {
        this.wrapper.setPrivacySchema(request.parseArgs(PrivacySchema.class));
        HillviewLogger.instance.info("Updated privacy schema");
        PrecomputedSketch<ITable, JsonString> empty =
                new PrecomputedSketch<ITable, JsonString>(new JsonString("{}"));
        this.runCompleteSketch(this.table, empty, request, context);
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        PostProcessedSketch<ITable, TableSummary, DPWrapper.PrivacySummary> post =
                new PostProcessedSketch<ITable, TableSummary, DPWrapper.PrivacySummary>(
                        new PrecomputedSketch<ITable, TableSummary>(summary)) {
                    @Override
                    public DPWrapper.PrivacySummary postProcess(@Nullable TableSummary result) {
                        return PrivateSimpleDBTarget.this.wrapper.addPrivateMetadata(
                                Converters.checkNull(result));
                    }
                };
        this.runCompleteSketch(this.table, post, request, context);
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

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getDataQuantiles3D(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles3D(request, context, this);
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;

        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        ColumnQuantization quantization = this.getPrivacySchema().quantization(cd.name);
        double epsilon = this.getPrivacySchema().epsilon(cd.name);
        if (quantization == null)
            throw new RuntimeException("No quantization information for column " + cd.name);

        IntervalDecomposition d0 = info[0].getDecomposition(quantization);
        IntervalDecomposition d1 = info[1].getDecomposition(quantization);
        Histogram histo = this.database.histogram(
                cd, info[0].getBuckets(quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        Histogram cdf = this.database.histogram(
                cd, info[1].getBuckets(quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        ISketch<ITable, Histogram> preHisto = new PrecomputedSketch<ITable, Histogram>(histo);
        ISketch<ITable, Histogram> preCdf = new PrecomputedSketch<ITable, Histogram>(cdf);
        int colIindex = this.wrapper.getColumnIndex(cd.name);
        DPHistogram privateHisto = new DPHistogram(preHisto, colIindex, d0, epsilon, false, this.wrapper.laplace);
        DPHistogram privateCdf = new DPHistogram(preCdf, colIindex, d1, epsilon, true, this.wrapper.laplace);
        ConcurrentPostprocessedSketch<ITable, Histogram, Histogram, Histogram, Histogram> cc =
                new ConcurrentPostprocessedSketch<ITable, Histogram, Histogram, Histogram, Histogram>(
                        privateHisto, privateCdf);
        this.runCompleteSketch(this.table, cc, request, context);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            try {
                IPrivateDataset result = new PrivateSimpleDBTarget(this, c);
                result.getWrapper().filter(filter);
                return result;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, request, context);
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPairDescription filter = request.parseArgs(RangeFilterPairDescription.class);
        if (filter.first.complement || filter.second.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            try {
                IPrivateDataset result = new PrivateSimpleDBTarget(this, c);
                result.getWrapper().filter(filter.first);
                result.getWrapper().filter(filter.second);
                return result;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        int result = this.database.distinctCount(col.columnName, this.wrapper.columnLimits);
        double epsilon = this.wrapper.getPrivacySchema().epsilon(col.columnName);
        Noise noise = DPWrapper.computeCountNoise(this.wrapper.getColumnIndex(col.columnName),
                DPWrapper.SpecialBucket.DistinctCount, epsilon, this.wrapper.laplace);
        CountWithConfidence dc = new CountWithConfidence(result).add(noise);
        ISketch<ITable, CountWithConfidence> sk = new PrecomputedSketch<ITable, CountWithConfidence>(dc);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        Heatmap heatmap = this.database.heatmap(
                info[0].cd, info[1].cd,
                info[0].getBuckets(), info[1].getBuckets(),
                this.wrapper.columnLimits,
                null, null);
        double epsilon = this.getPrivacySchema().epsilon(info[0].cd.name, info[1].cd.name);
        ColumnQuantization q0 = this.getPrivacySchema().quantization(info[0].cd.name);
        ColumnQuantization q1 = this.getPrivacySchema().quantization(info[1].cd.name);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        IntervalDecomposition d0 = info[0].getDecomposition(q0);
        IntervalDecomposition d1 = info[1].getDecomposition(q1);
        ISketch<ITable, Heatmap> sk = new PrecomputedSketch<ITable, Heatmap>(heatmap);
        DPHeatmapSketch noisyHeatmap = new DPHeatmapSketch(
                sk,  this.wrapper.getColumnIndex(info[0].cd.name, info[1].cd.name),
                d0, d1, epsilon, this.wrapper.laplace);
        this.runSketch(this.table, noisyHeatmap, request, context);
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        TableTarget.NextKArgs nextKArgs = request.parseArgs(TableTarget.NextKArgs.class);
        // Only allow this if the sort order is empty
        if (nextKArgs.order.getSize() != 0)
            throw new HillviewException("No column data can be displayed privately");
        NextKList result = new NextKList(new SmallTable(nextKArgs.order.toSchema()),
            null, new IntArrayList(), 0, 0);
        PrecomputedSketch<ITable, NextKList> nk = new PrecomputedSketch<ITable, NextKList>(result);
        this.runSketch(this.table, nk, request, context);
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
