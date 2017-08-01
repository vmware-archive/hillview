/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hillview;

import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.FilterMap;
import org.hillview.sketches.*;
import org.hillview.table.*;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.function.Function;

@SuppressWarnings("CanBeFinal")
public final class TableTarget extends RpcTarget {
    private final IDataSet<ITable> table;
    TableTarget(IDataSet<ITable> table) {
        this.table = table;
    }

    @HillviewRpc
    void getSchema(RpcRequest request, Session session) {
        SummarySketch ss = new SummarySketch();
        this.runSketch(this.table, ss, request, session);
    }

    static class NextKArgs {
        final RecordOrder order = new RecordOrder();
        @Nullable
        Object[] firstRow;
        int rowsOnScreen;
    }

    @HillviewRpc
    void getNextK(RpcRequest request, Session session) {
        NextKArgs nextKArgs = request.parseArgs(NextKArgs.class);
        RowSnapshot rs = null;
        if (nextKArgs.firstRow != null) {
            Schema schema = nextKArgs.order.toSchema();
            rs = RowSnapshot.parse(schema, nextKArgs.firstRow);
        }
        NextKSketch nk = new NextKSketch(nextKArgs.order, rs, nextKArgs.rowsOnScreen);
        this.runSketch(this.table, nk, request, session);
    }

    @HillviewRpc
    void uniqueStrings(RpcRequest request, Session session) {
        String columnName = request.parseArgs(String.class);
        DistinctStringsSketch sk = new DistinctStringsSketch(0, columnName);
        this.runCompleteSketch(this.table, sk, e->e, request, session);
    }

    @HillviewRpc
    void histogram(RpcRequest request, Session session) {
        ColumnAndRange info = request.parseArgs(ColumnAndRange.class);
        // TODO: use height in histogram computation
        int cdfBucketCount = info.cdfBucketCount;
        if (info.min >= info.max) {
            cdfBucketCount = 1;
            info.bucketCount = 1;
        }
        IStringConverter converter = null;
        if (info.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    info.bucketBoundaries, (int)Math.ceil(info.min), (int)Math.floor(info.max));
        BucketsDescriptionEqSize cdfBuckets = new BucketsDescriptionEqSize(info.min, info.max, cdfBucketCount);
        HistogramSketch cdf = new HistogramSketch(cdfBuckets, info.columnName, converter);
        ColumnAndRange.HistogramParts parts = info.prepare();
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(cdf, parts.sketch);
        this.runSketch(this.table, csk, request, session);
    }

    @HillviewRpc
    void heatMap(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();

        HeatMapSketch sk = new HeatMapSketch(h1.buckets, h2.buckets, h1.converter, h2.converter,
                info.first.columnName, info.second.columnName);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void histogram2D(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        HeatMapSketch sketch = new HeatMapSketch(h1.buckets, h2.buckets, h1.converter, h2.converter,
                info.first.columnName, info.second.columnName);

        int width = info.first.cdfBucketCount;
        if (info.first.min >= info.first.max)
            width = 1;
        BucketsDescriptionEqSize cdfBuckets =
                new BucketsDescriptionEqSize(info.first.min, info.first.max, width);
        HistogramSketch cdf = new HistogramSketch(cdfBuckets, info.first.columnName, null);

        ConcurrentSketch<ITable, Histogram, HeatMap> csk =
                new ConcurrentSketch<ITable, Histogram, HeatMap>(cdf, sketch);
        this.runSketch(this.table, csk, request, session);
    }

    static class RangeInfo {
        String columnName = "";
        // The following are only used for categorical columns
        int firstIndex;
        int lastIndex;
        @Nullable
        String firstValue;
        @Nullable
        String lastValue;

        @Nullable
        IStringConverter getConverter() {
            if (this.firstValue == null)
                return null;
            return new SortedStringsConverter(
                        new String[] { this.firstValue, this.lastValue }, this.firstIndex, this.lastIndex);
        }
    }

    @HillviewRpc
    void range(RpcRequest request, Session session) {
        RangeInfo info = request.parseArgs(RangeInfo.class);
        BasicColStatSketch sk = new BasicColStatSketch(info.columnName, info.getConverter(), 0, 1.0);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void range2D(RpcRequest request, Session session) {
        RangeInfo[] cols = request.parseArgs(RangeInfo[].class);
        if (cols.length != 2)
            throw new RuntimeException("Expected 2 RangeInfo objects, got " + cols.length);
        BasicColStatSketch sk1 = new BasicColStatSketch(cols[0].columnName, cols[0].getConverter(), 0, 1.0);
        BasicColStatSketch sk2 = new BasicColStatSketch(cols[1].columnName, cols[1].getConverter(), 0, 1.0);
        ConcurrentSketch<ITable, BasicColStats, BasicColStats> csk =
                new ConcurrentSketch<ITable, BasicColStats, BasicColStats>(sk1, sk2);
        this.runSketch(this.table, csk, request, session);
    }

    @HillviewRpc
    void filterEquality(RpcRequest request, Session session) {
        EqualityFilterDescription info = request.parseArgs(EqualityFilterDescription.class);
        EqualityFilter equalityFilter = new EqualityFilter(info.columnName, info.compareValue);
        FilterMap filterMap = new FilterMap(equalityFilter);
        this.runMap(this.table, filterMap, TableTarget::new, request, session);
    }

    @HillviewRpc
    void filterRange(RpcRequest request, Session session) {
        FilterDescription info = request.parseArgs(FilterDescription.class);
        RangeFilter filter = new RangeFilter(info);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, session);
    }

    @HillviewRpc
    void filter2DRange(RpcRequest request, Session session) {
        FilterPair info = request.parseArgs(FilterPair.class);
        Range2DFilter filter = new Range2DFilter(info);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, session);
    }

    static class QuantileInfo {
        int precision;
        double position;
        long tableSize;
        RecordOrder order = new RecordOrder();
    }

    @HillviewRpc
    void quantile(RpcRequest request, Session session) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(info.order, info.precision, info.tableSize);
        Function<SampleList, RowSnapshot> getRow = ql -> ql.getRow(info.position);
        this.runCompleteSketch(this.table, sk, getRow, request, session);
    }

    @HillviewRpc
    void heavyHitters(RpcRequest request, Session session) {
        // TODO: read size from client
        Schema schema = request.parseArgs(Schema.class);
        Converters.checkNull(schema);
        FreqKSketch sk = new FreqKSketch(schema, 100);
        this.runCompleteSketch(this.table, sk, HeavyHittersTarget::new, request, session);
    }

    static class HeavyHittersFilterInfo {
        String hittersId = "";
        @Nullable
        Schema schema;
    }

    @HillviewRpc
    void filterHeavy(RpcRequest request, Session session) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        RpcTarget target = RpcObjectManager.instance.getObject(hhi.hittersId);
        HeavyHittersTarget hht = (HeavyHittersTarget)target;
        TableFilter filter = hht.heavyHitters.heavyFilter(Converters.checkNull(hhi.schema));
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, session);
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }
}
