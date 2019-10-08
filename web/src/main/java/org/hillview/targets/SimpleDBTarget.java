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
import org.hillview.sketches.*;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.FreqKList;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.TableSummary;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
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
public final class SimpleDBTarget extends RpcTarget {
    private final JdbcConnectionInformation jdbc;
    private final JdbcDatabase database;
    private final int rowCount;
    @Nullable
    private Schema schema;
    private final IDataSet<ITable> unused;

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
        try {
            this.database.connect();
            this.rowCount = this.database.getRowCount();
            this.schema = this.database.getSchema();
            this.database.disconnect();
            SmallTable empty = new SmallTable(this.schema);
            this.unused = new LocalDataSet<ITable>(empty);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> ISketch<ITable, T> makeSketch(T data, @Nullable ISketch<ITable, T> sk) {
        return new PrecomputedSketch<ITable, T>(data, sk);
    }

    private <T> ISketch<ITable, T> makeSketch(T data) {
        return new PrecomputedSketch<ITable, T>(data, null);
    }

    @Override
    public String toString() {
        return "Local database: " + this.jdbc.toString();
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        TableSummary summary = new TableSummary(this.schema, this.rowCount);
        this.runSketch(this.unused, this.makeSketch(summary, new SummarySketch()), request, context);
    }

    static class DistinctCount implements IJson {
        public final int distinctItemCount;

        DistinctCount(int dc) {
            this.distinctItemCount = dc;
        }
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        try {
            this.database.connect();
            int result = this.database.distinctCount(col.columnName);
            this.database.disconnect();
            DistinctCount dc = new DistinctCount(result);
            ISketch<ITable, DistinctCount> sk = this.makeSketch(dc);
            this.runSketch(this.unused, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void heavyHitters(RpcRequest request, RpcRequestContext context) {
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
            ISketch<ITable, TopList> sk = this.makeSketch(result);
            this.runSketch(this.unused, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) {
        this.heavyHitters(request, context);
    }

    @HillviewRpc
    public void getDataRanges1D(RpcRequest request, RpcRequestContext context) {
        RangeArgs[] info = request.parseArgs(RangeArgs[].class);
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
            ISketch<ITable, JsonList<BucketsInfo>> sk = this.makeSketch(result);
            this.runSketch(this.unused, sk, request, context);
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
            Histogram histo = this.database.histogram(cd, info[0].getBuckets());
            Histogram cdf = this.database.histogram(cd, info[1].getBuckets());
            Pair<AugmentedHistogram, HistogramPrefixSum> result = new
                    Pair<AugmentedHistogram, HistogramPrefixSum>(
                            new AugmentedHistogram(histo), new HistogramPrefixSum(cdf));
            this.database.disconnect();
            ISketch<ITable, Pair<AugmentedHistogram, HistogramPrefixSum>> sk = this.makeSketch(result);
            this.runSketch(this.unused, sk, request, context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
