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
import org.hillview.dataStructures.DyadicDecomposition;
import org.hillview.dataStructures.HistogramRequestInfo;
import org.hillview.dataStructures.PrivateHeatmap;
import org.hillview.dataStructures.PrivateHistogram;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.IdMap;
import org.hillview.sketches.PrecomputedSketch;
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
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.JsonString;

import java.sql.SQLException;

public class PrivateSimpleDBTarget extends SimpleDBTarget implements IPrivateDataset {
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
        this.runCompleteSketch(this.table, empty, (d, c) -> d, request, context);
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
        ColumnQuantization quantization = this.getPrivacySchema().quantization(cd.name);
        double epsilon = this.getPrivacySchema().epsilon(cd.name);
        if (quantization == null)
            throw new RuntimeException("No quantization information for column " + cd.name);

        DyadicDecomposition d0 = info[0].getDecomposition(quantization);
        DyadicDecomposition d1 = info[1].getDecomposition(quantization);
        Histogram histo = this.database.histogram(
                cd, info[0].getBuckets(quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        Histogram cdf = this.database.histogram(
                cd, info[1].getBuckets(quantization), this.wrapper.columnLimits, quantization, this.rowCount);
        Pair<Histogram, Histogram> result = new Pair<Histogram, Histogram>(histo, cdf);
        ISketch<ITable, Pair<Histogram, Histogram>> sk = new PrecomputedSketch<ITable, Pair<Histogram, Histogram>>(result);
        this.runCompleteSketch(this.table, sk, (e, c) ->
                new Pair<PrivateHistogram, PrivateHistogram>(
                        new PrivateHistogram(d0, Converters.checkNull(e.first), epsilon, false, this.wrapper.laplace),
                        new PrivateHistogram(d1, Converters.checkNull(e.second), epsilon, true, this.wrapper.laplace)),
                request, context);
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
        DyadicDecomposition d0 = info[0].getDecomposition(q0);
        DyadicDecomposition d1 = info[1].getDecomposition(q1);
        PrivateHeatmap result = new PrivateHeatmap(d0, d1, heatmap, epsilon, this.wrapper.laplace);
        ISketch<ITable, Heatmap> sk = new PrecomputedSketch<ITable, Heatmap>(result.heatmap);
        this.runCompleteSketch(this.table, sk, (e, c) -> e, request, context);
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
