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
import org.hillview.sketches.highorder.ConcurrentPostprocessedSketch;
import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.maps.highorder.IdMap;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.results.*;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.filters.RangeFilterArrayDescription;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.utils.*;

import java.sql.SQLException;

public class PrivateSimpleDBTarget extends SimpleDBTarget implements IPrivateDataset {
    static final long serialVersionUID = 1;

    // This class inherits columnLimits from SimpleDBTarget and it has another one
    // in the wrapper.  We only use the one in the wrapper.
    private final DPWrapper wrapper;

    PrivateSimpleDBTarget(JdbcConnectionInformation conn, HillviewComputation c,
                          PrivacySchema privacySchema, String schemaFilename) throws SQLException {
        super(conn, c, schemaFilename);
        this.wrapper = new DPWrapper(privacySchema, schemaFilename);
        this.database.connect();
    }

    private PrivateSimpleDBTarget(PrivateSimpleDBTarget other, HillviewComputation computation) throws SQLException {
        super(other.jdbc, computation, other.metadataDirectory);
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
        this.returnResult(new JsonInString("{}"), request, context);
    }

    @HillviewRpc
    public void getMetadata(RpcRequest request, RpcRequestContext context) {
        GeoFileInformation[] info = this.getGeoFileInformation();
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        PostProcessedSketch<ITable, TableSummary, DPWrapper.TableMetadata> post =
            new PrecomputedSketch<ITable, TableSummary>(summary).andThen(
                    s -> new TableMetadata(s, info)).andThen(
                            PrivateSimpleDBTarget.this.wrapper::addPrivateMetadata);
        this.runCompleteSketch(this.table, post, request, context);
    }

    @Override
    public String toString() {
        return "Private local database: " + this.jdbc.toString();
    }

    @HillviewRpc
    public void getDataQuantiles(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles(request, context, this);
    }

    @HillviewRpc
    public void histogramAndCDF(RpcRequest request, RpcRequestContext context) throws SQLException {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;

        ColumnDescription cd = info.histos[0].cd;  // both args should be on the same column
        ColumnQuantization quantization = this.getPrivacySchema().quantization(cd.name);
        double epsilon = this.getPrivacySchema().epsilon(cd.name);
        if (quantization == null)
            throw new RuntimeException("No quantization information for column " + cd.name);

        IntervalDecomposition d0 = info.getDecomposition(0, quantization);
        IntervalDecomposition d1 = info.getDecomposition(1, quantization);
        JsonGroups<Count> histo = this.database.histogram(
                cd, info.getBuckets(0, quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        JsonGroups<Count> cdf = this.database.histogram(
                cd, info.getBuckets(1, quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        ISketch<ITable, JsonGroups<Count>> preHisto = new PrecomputedSketch<>(histo);
        ISketch<ITable, JsonGroups<Count>> preCdf = new PrecomputedSketch<>(cdf);
        int colIindex = this.wrapper.getColumnIndex(cd.name);
        DPHistogram<JsonGroups<Count>> privateHisto = new DPHistogram<>(preHisto, colIindex, d0, epsilon, false, this.wrapper.laplace);
        DPHistogram<JsonGroups<Count>> privateCdf = new DPHistogram<>(preCdf, colIindex, d1, epsilon, true, this.wrapper.laplace);
        ConcurrentPostprocessedSketch<ITable, JsonGroups<Count>, JsonGroups<Count>, Two<JsonGroups<Count>>, Two<JsonGroups<Count>>> cc =
                new ConcurrentPostprocessedSketch<>(privateHisto, privateCdf);
        this.runCompleteSketch(this.table, cc, request, context);
    }

    @HillviewRpc
    public void filterRanges(RpcRequest request, RpcRequestContext context) {
        RangeFilterArrayDescription filter = request.parseArgs(RangeFilterArrayDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            try {
                IPrivateDataset result = new PrivateSimpleDBTarget(this, c);
                for (RangeFilterDescription f: filter.filters)
                     result.getWrapper().filter(f);
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
        this.returnResult(dc, request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) throws SQLException {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        JsonGroups<JsonGroups<Count>> heatmap = this.database.histogram2D(
                info.histos[0].cd, info.histos[1].cd,
                info.getBuckets(0), info.getBuckets(1),
                this.wrapper.columnLimits,
                null, null);
        double epsilon = this.getPrivacySchema().epsilon(
                info.histos[0].cd.name, info.histos[1].cd.name);
        ColumnQuantization q0 = this.getPrivacySchema().quantization(info.histos[0].cd.name);
        ColumnQuantization q1 = this.getPrivacySchema().quantization(info.histos[1].cd.name);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        IntervalDecomposition d0 = info.getDecomposition(0, q0);
        IntervalDecomposition d1 = info.getDecomposition(1, q1);
        ISketch<ITable, JsonGroups<JsonGroups<Count>>> sk = new PrecomputedSketch<>(heatmap);
        DPHeatmapSketch<JsonGroups<Count>, JsonGroups<JsonGroups<Count>>> noisyHeatmap =
                new DPHeatmapSketch<>(
                sk, this.wrapper.getColumnIndex(info.histos[0].cd.name, info.histos[1].cd.name),
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
        this.returnResult(result, request, context);
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
