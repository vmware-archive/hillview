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
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.IdMap;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.storage.ColumnnFilters;
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
import org.hillview.utils.HillviewException;
import org.hillview.utils.JsonList;

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
    final JdbcConnectionInformation jdbc;
    protected final JdbcDatabase database;
    protected final int rowCount;
    @Nullable
    protected Schema schema;
    // This table is actually not used for anything; the only purpose
    // is for some APIs to be similar to the TableTarget class.
    protected final IDataSet<ITable> table;
    private final ColumnnFilters columnLimits;

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
        this.columnLimits = new ColumnnFilters();
        try {
            this.database.connect();
            this.rowCount = this.database.getRowCount(this.columnLimits);
            this.schema = this.database.getSchema();
            this.database.disconnect();
            SmallTable empty = new SmallTable(this.schema);
            this.table = new LocalDataSet<ITable>(empty);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    SimpleDBTarget(SimpleDBTarget other, HillviewComputation computation) {
        super(computation);
        this.jdbc = other.jdbc;
        this.schema = other.schema;
        this.registerObject();
        this.database = new JdbcDatabase(this.jdbc);
        this.columnLimits = new ColumnnFilters(other.columnLimits);
        this.table = other.table;
        try {
            this.database.connect();
            this.rowCount = this.database.getRowCount(this.columnLimits);
            this.database.disconnect();
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

    static class DistinctCount implements IJson {
        final int distinctItemCount;
        DistinctCount(int dc) {
            this.distinctItemCount = dc;
        }
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        try {
            this.database.connect();
            int result = this.database.distinctCount(col.columnName, this.columnLimits);
            // TODO(pratiksha): add noise to this count
            this.database.disconnect();
            DistinctCount dc = new DistinctCount(result);
            ISketch<ITable, DistinctCount> sk = new PrecomputedSketch<ITable, DistinctCount>(dc);
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        try {
            this.database.connect();
            SmallTable tbl = this.database.topFreq(
                    info.columns, (int)Math.ceil(info.amount * info.totalRows / 100));
            List<String> cols = tbl.getSchema().getColumnNames();
            String lastCol = cols.get(cols.size() - 1);
            Object2IntOpenHashMap<RowSnapshot> map = new Object2IntOpenHashMap<RowSnapshot>();
            for (int i = 0; i < tbl.getNumOfRows(); i++) {
                RowSnapshot rs = new RowSnapshot(tbl, i);
                RowSnapshot proj = new RowSnapshot(rs, info.columns);
                map.put(proj, (int)rs.getDouble(lastCol));
            }
            this.database.disconnect();
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //@HillviewRpc // TODO: we don't know how to do this privately
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    //@HillviewRpc // TODO: we don't know how to do this privately
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void getDataQuantiles1D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] info = request.parseArgs(QuantilesArgs[].class);
        assert info.length == 1;
        try {
            BucketsInfo range;
            this.database.connect();
            if (info[0].cd.kind == ContentsKind.Integer ||
                    info[0].cd.kind == ContentsKind.Double ||
                    info[0].cd.kind == ContentsKind.Date) {
                range = this.database.numericDataRange(info[0].cd);
            } else {
                range = this.database.stringBuckets(info[0].cd, info[0].stringsToSample);
            }
            this.database.disconnect();
            JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
            result.add(range);
            ISketch<ITable, JsonList<BucketsInfo>> sk = new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(result);
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void getDataQuantiles2D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] info = request.parseArgs(QuantilesArgs[].class);
        assert info.length == 2;
        try {
            this.database.connect();
            JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(2);
            for (int i = 0; i < 2; i++) {
                BucketsInfo range;
                if (info[i].cd.kind == ContentsKind.Integer ||
                        info[i].cd.kind == ContentsKind.Double ||
                        info[i].cd.kind == ContentsKind.Date) {
                    range = this.database.numericDataRange(info[i].cd);
                } else {
                    range = this.database.stringBuckets(info[i].cd, info[i].stringsToSample);
                }
                result.add(range);
            }
            this.database.disconnect();
            ISketch<ITable, JsonList<BucketsInfo>> sk = new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(result);
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnDescription cd = info[0].cd;  // both args should be on the same column
        try {
            this.database.connect();
            @Nullable
            RangeFilterDescription filter = this.columnLimits.get(cd.name);
            Histogram histo = this.database.histogram(cd, info[0].getBuckets(), filter, null, this.rowCount);
            Histogram cdf = this.database.histogram(cd, info[1].getBuckets(), filter, null, this.rowCount);
            Pair<AugmentedHistogram, HistogramPrefixSum> result = new
                    Pair<AugmentedHistogram, HistogramPrefixSum>(
                            new AugmentedHistogram(histo), new HistogramPrefixSum(cdf));
            this.database.disconnect();
            ISketch<ITable, Pair<AugmentedHistogram, HistogramPrefixSum>> sk = new PrecomputedSketch<ITable, Pair<AugmentedHistogram, HistogramPrefixSum>>(result);
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
            @Nullable
            RangeFilterDescription filter0 = this.columnLimits.get(info[0].cd.name);
            @Nullable
            RangeFilterDescription filter1 = this.columnLimits.get(info[1].cd.name);
            Heatmap heatmap = this.database.heatmap(
                    info[0].cd, info[1].cd,
                    info[0].getBuckets(), info[1].getBuckets(),
                    filter0, filter1,
                    null, null);
            this.database.disconnect();
            ISketch<ITable, Heatmap> sk = new PrecomputedSketch<ITable, Heatmap>(heatmap);
            this.runSketch(this.table, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
