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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.table.filters.RangeFilterArrayDescription;
import org.hillview.maps.highorder.IdMap;
import org.hillview.sketches.results.*;
import org.hillview.storage.ColumnLimits;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.storage.jdbc.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * This targets represents a simple database that is accessed directly using SQL from
 * the front-end.  Note that all operations on the local database are not scalable -
 * they are not expected to scale to billions of rows.
 */
public class SimpleDBTarget extends TableRpcTarget {
    static final long serialVersionUID = 1;

    final JdbcConnectionInformation jdbc;
    protected final JdbcDatabase database;
    protected final int rowCount;
    @Nullable
    protected Schema schema;
    private final ColumnLimits columnLimits;

    static {
        try {
            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver ());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    SimpleDBTarget(JdbcConnectionInformation jdbc, HillviewComputation computation, @Nullable String metadataDirectory) {
        super(computation, metadataDirectory);
        this.jdbc = jdbc;
        this.schema = null;
        this.registerObject();
        this.database = new JdbcDatabase(this.jdbc);
        this.columnLimits = new ColumnLimits();

        try {
            this.database.connect();
            this.rowCount = this.database.getRowCount(this.columnLimits);
            this.schema = this.database.getSchema();
            // The table table is actually not used for anything; the only purpose
            // is for some APIs to be similar to the TableTarget class.
            SmallTable empty = new SmallTable(this.schema);
            this.setTable(new LocalDataSet<ITable>(empty));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "Local database: " + this.jdbc.database;
    }

    @HillviewRpc
    public void getMetadata(RpcRequest request, RpcRequestContext context) {
        GeoFileInformation[] info = this.getGeoFileInformation();
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        this.returnResult(new TableMetadata(summary, info), request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        int result = this.database.distinctCount(col.columnName, this.columnLimits);
        CountWithConfidence dc = new CountWithConfidence(result);
        this.returnResult(dc, request, context);
    }

    private void heavyHitters(RpcRequest request, RpcRequestContext context) throws SQLException {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        SmallTable tbl = this.database.topFreq(
                info.columns, Converters.toInt(Math.ceil(info.amount * info.totalRows / 100)),
                this.columnLimits);
        List<String> cols = tbl.getSchema().getColumnNames();
        String lastCol = cols.get(cols.size() - 1);
        Object2IntOpenHashMap<RowSnapshot> map = new Object2IntOpenHashMap<RowSnapshot>();
        for (int i = 0; i < tbl.getNumOfRows(); i++) {
            RowSnapshot rs = new RowSnapshot(tbl, i);
            RowSnapshot proj = new RowSnapshot(rs, info.columns);
            map.put(proj, Converters.toInt(rs.getDouble(lastCol)));
        }
        FreqKList fkList = new FreqKList(info.totalRows, 0, map);
        fkList.sortList();
        HillviewComputation computation;
        if (context.computation != null)
            computation = context.computation;
        else
            computation = new HillviewComputation(null, request);
        HeavyHittersTarget hht = new HeavyHittersTarget(fkList, computation);
        TopList result = new TopList(fkList.sortTopK(info.columns), hht.getId().toString());
        this.returnResult(result, request, context);
    }

    @HillviewRpc
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) throws SQLException {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) throws SQLException {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void getDataQuantiles(RpcRequest request, RpcRequestContext context) throws SQLException {
        QuantilesArgs[] info = request.parseArgs(QuantilesArgs[].class);
        JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(info.length);
        for (QuantilesArgs quantilesArgs : info) {
            BucketsInfo range;
            if (quantilesArgs.cd.kind == ContentsKind.Integer ||
                    quantilesArgs.cd.kind == ContentsKind.Double ||
                    quantilesArgs.cd.kind == ContentsKind.Date ||
                    quantilesArgs.cd.kind == ContentsKind.LocalDate ||
                    quantilesArgs.cd.kind == ContentsKind.Time) {
                range = this.database.numericDataRange(quantilesArgs.cd, this.columnLimits);
            } else {
                range = this.database.stringBuckets(
                        quantilesArgs.cd, quantilesArgs.stringsToSample, this.columnLimits);
            }
            result.add(range);
        }
        this.returnResult(result, request, context);
    }

    @HillviewRpc
    public void histogramAndCDF(RpcRequest request, RpcRequestContext context) throws SQLException {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        ColumnDescription cd = info.histos[0].cd;  // both args should be on the same column
        @Nullable
        JsonGroups<Count> histo = this.database.histogram(
                cd, info.getBuckets(0), this.columnLimits, null, this.rowCount);
        JsonGroups<Count> cdf = this.database.histogram(
                cd, info.getBuckets(1), this.columnLimits, null, this.rowCount);
        Two<Two<JsonGroups<Count>>> result = new Two<>(
                new Two<>(histo), new Two<>(cdf.prefixSum(Count::add, JsonGroups::new)));
        this.returnResult(result, request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) throws SQLException {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        JsonGroups<JsonGroups<Count>> heatmap = this.database.histogram2D(
                info.histos[0].cd, info.histos[1].cd,
                info.getBuckets(0), info.getBuckets(1),
                this.columnLimits,
                null, null);
        this.returnResult(heatmap, request, context);
    }

    @HillviewRpc
    public void filterRanges(RpcRequest request, RpcRequestContext context) {
        RangeFilterArrayDescription filter = request.parseArgs(RangeFilterArrayDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c1) -> {
            SimpleDBTarget result = new SimpleDBTarget(this.jdbc, c1, this.metadataDirectory);
            for (RangeFilterDescription f: filter.filters)
                result.columnLimits.intersect(f);
            return result;
        }, request, context);
    }
}
