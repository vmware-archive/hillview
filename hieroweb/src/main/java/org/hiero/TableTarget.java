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

package org.hiero;

import org.hiero.dataset.ConcurrentSketch;
import org.hiero.dataset.TripleSketch;
import org.hiero.dataset.api.IDataSet;
import org.hiero.maps.FilterMap;
import org.hiero.sketches.*;
import org.hiero.table.*;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IStringConverter;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.Serializable;
import java.util.function.Function;

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
        RecordOrder order = new RecordOrder();
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

    static class HistogramParts {
        HistogramParts(BucketsDescriptionEqSize buckets, @Nullable IStringConverter converter,
                       Hist1DLightSketch sketch) {
            this.buckets = buckets;
            this.converter = converter;
            this.sketch = sketch;
        }

        final BucketsDescriptionEqSize buckets;
        @Nullable
        final IStringConverter converter;
        final Hist1DLightSketch sketch;
    }

    static class ColumnAndRange implements Serializable {
        String columnName = "";
        double min;
        double max;
        int cdfBucketCount;
        int bucketCount;  // only used for histogram
        @Nullable
        String[] bucketBoundaries;  // only used for Categorical columns histograms

        HistogramParts prepare() {
            IStringConverter converter = null;
            if (bucketBoundaries != null)
                converter = new SortedStringsConverter(
                        bucketBoundaries, (int)Math.ceil(min), (int)Math.floor(max));
            BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(min, max, bucketCount);
            Hist1DLightSketch sketch = new Hist1DLightSketch(buckets, columnName, converter);
            return new HistogramParts(buckets, converter, sketch);
        }
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
        Hist1DLightSketch cdf = new Hist1DLightSketch(cdfBuckets, info.columnName, converter);
        HistogramParts parts = info.prepare();
        ConcurrentSketch<ITable, Histogram1DLight, Histogram1DLight> csk =
                new ConcurrentSketch<ITable, Histogram1DLight, Histogram1DLight>(cdf, parts.sketch);
        this.runSketch(this.table, csk, request, session);
    }

    static class ColPair {
        @Nullable
        ColumnAndRange first;
        @Nullable
        ColumnAndRange second;
    }

    @HillviewRpc
    void heatMap(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        HistogramParts h1 = Converters.checkNull(info.first).prepare();
        HistogramParts h2 = Converters.checkNull(info.second).prepare();

        HeatMapSketch sk = new HeatMapSketch(h1.buckets, h2.buckets, h1.converter, h2.converter,
                info.first.columnName, info.second.columnName);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void histogram2D(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        HistogramParts h1 = Converters.checkNull(info.first).prepare();
        HistogramParts h2 = Converters.checkNull(info.second).prepare();
        int width = info.first.cdfBucketCount;
        if (info.first.min >= info.first.max)
            width = 1;
        BucketsDescriptionEqSize cdfBuckets =
                new BucketsDescriptionEqSize(info.first.min, info.first.max, width);
        Hist1DLightSketch cdf = new Hist1DLightSketch(cdfBuckets, info.first.columnName, null);

        TripleSketch<ITable, Histogram1DLight, Histogram1DLight, Histogram1DLight> csk =
                new TripleSketch<ITable, Histogram1DLight, Histogram1DLight, Histogram1DLight>(cdf, h1.sketch, h2.sketch);
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
            if (firstValue == null)
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

    static class RangeFilter implements TableFilter, Serializable {
        final ColumnAndRange args;
        @Nullable
        IColumn column;  // not really nullable, but set later.
        @Nullable
        final IStringConverter converter;

        RangeFilter(ColumnAndRange args) {
            this.args = args;
            this.column = null;
            if (args.bucketBoundaries != null)
                this.converter = new SortedStringsConverter(
                        args.bucketBoundaries, (int)Math.ceil(args.min), (int)Math.floor(args.max));
            else
                this.converter = null;
        }

        @Override
        public void setTable(ITable table) {
            IColumn col = table.getColumn(this.args.columnName);
            this.column = Converters.checkNull(col);
        }

        public boolean test(int rowIndex) {
            if (Converters.checkNull(this.column).isMissing(rowIndex))
                return false;
            double d = this.column.asDouble(rowIndex, this.converter);
            return this.args.min <= d && d <= this.args.max;
        }
    }

    @HillviewRpc
    void filterRange(RpcRequest request, Session session) {
        ColumnAndRange info = request.parseArgs(ColumnAndRange.class);
        RangeFilter filter = new RangeFilter(info);
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
