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
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.*;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * This targets represents a simple database that is accessed directly using SQL from
 * the front-end.
 */
public final class SimpleDBTarget extends RpcTarget {
    private final JdbcConnectionInformation jdbc;
    private final JdbcDatabase database;
    private final int rowCount;
    @Nullable
    private Schema schema;

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
        SummarySketch.TableSummary summary = new SummarySketch.TableSummary(this.schema, this.rowCount);
        this.returnResultDirect(request, context, summary);
    }

    static class HLogLogInfo {
        String columnName = "";
        long seed;
    }

    static class DistinctCount implements IJson {
        public final int distinctItemCount;

        DistinctCount(int dc) {
            this.distinctItemCount = dc;
        }
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        TableTarget.HLogLogInfo col = request.parseArgs(TableTarget.HLogLogInfo.class);
        try {
            this.database.connect();
            int result = this.database.distinctCount(col.columnName);
            this.database.disconnect();
            DistinctCount dc = new DistinctCount(result);
            this.returnResultDirect(request, context, dc);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void heavyHitters(RpcRequest request, RpcRequestContext context) {
        TableTarget.HeavyHittersInfo info = request.parseArgs(TableTarget.HeavyHittersInfo.class);
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
            this.returnResultDirect(request, context, result);
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
        if (info[0].cd.kind == ContentsKind.Integer || info[0].cd.kind == ContentsKind.Double) {
            try {
                this.database.connect();
                DataRange range = this.database.numericDataRange(info[0].cd);
                this.database.disconnect();
                JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
                result.add(range);
                this.returnResultDirect(request, context, result);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Not yet implemented");
        }
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramArgs info = request.parseArgs(HistogramArgs.class);
        if (info.cd.kind == ContentsKind.Integer || info.cd.kind == ContentsKind.Double) {
            try {
                this.database.connect();
                Histogram result = this.database.numericHistogram(
                        info.cd, (DoubleHistogramBuckets)info.getBuckets());
                this.database.disconnect();
                this.returnResultDirect(request, context, result);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("String columns not yet supported");
        }
    }
}
