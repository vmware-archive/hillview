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
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.utils.Pair;
import org.hillview.maps.highorder.IdMap;
import org.hillview.sketches.results.*;
import org.hillview.storage.ColumnLimits;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.filters.RangeFilterPairDescription;
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
public class SimpleDBTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    final JdbcConnectionInformation jdbc;
    protected final JdbcDatabase database;
    protected final int rowCount;
    @Nullable
    protected Schema schema;
    // This table is actually not used for anything; the only purpose
    // is for some APIs to be similar to the TableTarget class.
    protected final IDataSet<ITable> table;
    private final ColumnLimits columnLimits;

    static {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver ());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    SimpleDBTarget(JdbcConnectionInformation jdbc, HillviewComputation computation) {
        super(computation);
        this.jdbc = jdbc;
        this.schema = null;
        this.registerObject();
        this.database = new JdbcDatabase(this.jdbc);
        this.columnLimits = new ColumnLimits();

        try {
            this.database.connect();
            this.rowCount = this.database.getRowCount(this.columnLimits);
            this.schema = this.database.getSchema();
            SmallTable empty = new SmallTable(this.schema);
            this.table = new LocalDataSet<ITable>(empty);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "Local database: " + this.jdbc.toString();
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        this.runSketch(this.table, new PrecomputedSketch<ITable, TableSummary>(summary), request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        int result = this.database.distinctCount(col.columnName, this.columnLimits);
        CountWithConfidence dc = new CountWithConfidence(result);
        ISketch<ITable, CountWithConfidence> sk = new PrecomputedSketch<ITable, CountWithConfidence>(dc);
        this.runSketch(this.table, sk, request, context);
    }

    private void heavyHitters(RpcRequest request, RpcRequestContext context) {
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
        ISketch<ITable, TopList> sk = new PrecomputedSketch<ITable, TopList>(result);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getDataQuantiles1D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] info = request.parseArgs(QuantilesArgs[].class);
        assert info.length == 1;
        ColumnDescription cd = info[0].cd;
        BucketsInfo range;
        if (cd.kind == ContentsKind.Integer ||
                cd.kind == ContentsKind.Double ||
                cd.kind == ContentsKind.Date) {
            range = this.database.numericDataRange(cd, this.columnLimits);
        } else {
            range = this.database.stringBuckets(
                    cd, info[0].stringsToSample, this.columnLimits);
        }
        JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
        result.add(range);
        ISketch<ITable, JsonList<BucketsInfo>> sk = new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(result);
        this.runSketch(this.table, sk, request, context);
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getDataQuantiles2D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] info = request.parseArgs(QuantilesArgs[].class);
        assert info.length == 2;
        JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(2);
        for (int i = 0; i < 2; i++) {
            BucketsInfo range;
            if (info[i].cd.kind == ContentsKind.Integer ||
                    info[i].cd.kind == ContentsKind.Double ||
                    info[i].cd.kind == ContentsKind.Date) {
                range = this.database.numericDataRange(info[i].cd, this.columnLimits);
            } else {
                range = this.database.stringBuckets(
                        info[i].cd, info[i].stringsToSample, this.columnLimits);
            }
            result.add(range);
        }
        ISketch<ITable, JsonList<BucketsInfo>> sk = new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(result);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void histogramAndCDF(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        @Nullable
        JsonGroups<Count> histo = this.database.histogram(
                cd, info[0].getBuckets(), this.columnLimits, null, this.rowCount);
        JsonGroups<Count> cdf = this.database.histogram(
                cd, info[1].getBuckets(), this.columnLimits, null, this.rowCount);
        Pair<JsonGroups<Count>, JsonGroups<Count>> result = new Pair<>(
                histo, cdf.prefixSum(Count::add, JsonGroups::new));
        ISketch<ITable, Pair<JsonGroups<Count>, JsonGroups<Count>>> sk =
                new PrecomputedSketch<>(result);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        JsonGroups<JsonGroups<Count>> heatmap = this.database.histogram2D(
                info[0].cd, info[1].cd,
                info[0].getBuckets(), info[1].getBuckets(),
                this.columnLimits,
                null, null);
        ISketch<ITable, JsonGroups<JsonGroups<Count>>> sk =
                new PrecomputedSketch<ITable, JsonGroups<JsonGroups<Count>>>(heatmap);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c1) -> {
            SimpleDBTarget result = new SimpleDBTarget(this.jdbc, c1);
            result.columnLimits.intersect(filter);
            return result;
        }, request, context);
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPairDescription filter = request.parseArgs(RangeFilterPairDescription.class);
        if (filter.first.complement || filter.second.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c1) -> {
            SimpleDBTarget result = new SimpleDBTarget(this.jdbc, c1);
            result.columnLimits.intersect(filter.first);
            result.columnLimits.intersect(filter.second);
            return result;
        }, request, context);
    }
}
