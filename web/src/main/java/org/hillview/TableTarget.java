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
import org.hillview.dataset.TripleSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.ConvertColumnMap;
import org.hillview.maps.FilterMap;
import org.hillview.maps.LinearProjectionMap;
import org.hillview.sketches.*;
import org.hillview.table.*;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.LinAlg;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        String[] columnNames = request.parseArgs(String[].class);
        DistinctStringsSketch sk = new DistinctStringsSketch(0, columnNames);
        this.runCompleteSketch(this.table, sk, e->e, request, session);
    }

    @HillviewRpc
    void histogram(RpcRequest request, Session session) {
        ColumnAndRange info = request.parseArgs(ColumnAndRange.class);
        int cdfBucketCount = info.cdfBucketCount;
        if (info.min >= info.max) {
            cdfBucketCount = 1;
            info.bucketCount = 1;
        }
        ColumnAndRange.HistogramParts parts = info.prepare();
        BucketsDescriptionEqSize cdfBuckets = new BucketsDescriptionEqSize(info.min, info.max, cdfBucketCount);
        HistogramSketch cdf = new HistogramSketch(cdfBuckets, parts.column);
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(cdf, parts.sketch);
        this.runSketch(this.table, csk, request, session);
    }

    @HillviewRpc
    void heatMap(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        HeatMapSketch sk = new HeatMapSketch(h1.buckets, h2.buckets, h1.column, h2.column);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void heatMap3D(RpcRequest request, Session session) {
        ColTriple info = request.parseArgs(ColTriple.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        ColumnAndRange.HistogramParts h3 = Converters.checkNull(info.third).prepare();
        HeatMap3DSketch sk = new HeatMap3DSketch(h1.buckets, h2.buckets, h3.buckets,
                h1.column, h2.column, h3.column);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void histogram2D(RpcRequest request, Session session) {
        ColPair info = request.parseArgs(ColPair.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        HeatMapSketch sketch = new HeatMapSketch(h1.buckets, h2.buckets, h1.column, h2.column);

        int width = info.first.cdfBucketCount;
        if (info.first.min >= info.first.max)
            width = 1;
        BucketsDescriptionEqSize cdfBuckets =
                new BucketsDescriptionEqSize(info.first.min, info.first.max, width);
        HistogramSketch cdf = new HistogramSketch(cdfBuckets, h1.column);
        ConcurrentSketch<ITable, Histogram, HeatMap> csk =
                new ConcurrentSketch<ITable, Histogram, HeatMap>(cdf, sketch);
        this.runSketch(this.table, csk, request, session);
    }

    static class RangeInfo {
        String columnName = "";
        // The following are only used for categorical columns
        @Nullable
        String[] allNames;

        ColumnNameAndConverter getColumn() {
            IStringConverter converter = null;
            if (this.allNames != null)
                converter = new SortedStringsConverter(this.allNames);
            return new ColumnNameAndConverter(this.columnName, converter);
        }

        BasicColStatSketch getBasicStatsSketch() {
            return new BasicColStatSketch(this.getColumn(), 0, 1.0);
        }
    }

    @HillviewRpc
    void range(RpcRequest request, Session session) {
        RangeInfo info = request.parseArgs(RangeInfo.class);
        BasicColStatSketch sk = new BasicColStatSketch(info.getColumn(), 0, 1.0);
        this.runSketch(this.table, sk, request, session);
    }

    @HillviewRpc
    void range2D(RpcRequest request, Session session) {
        RangeInfo[] cols = request.parseArgs(RangeInfo[].class);
        if (cols.length != 2)
            throw new RuntimeException("Expected 2 RangeInfo objects, got " + cols.length);
        BasicColStatSketch sk1 = cols[0].getBasicStatsSketch();
        BasicColStatSketch sk2 = cols[1].getBasicStatsSketch();
        ConcurrentSketch<ITable, BasicColStats, BasicColStats> csk =
                new ConcurrentSketch<ITable, BasicColStats, BasicColStats>(sk1, sk2);
        this.runSketch(this.table, csk, request, session);
    }

    @HillviewRpc
    void range3D(RpcRequest request, Session session) {
        RangeInfo[] cols = request.parseArgs(RangeInfo[].class);
        if (cols.length != 3)
            throw new RuntimeException("Expected 3 RangeInfo objects, got " + cols.length);
        BasicColStatSketch sk1 = cols[0].getBasicStatsSketch();
        BasicColStatSketch sk2 = cols[1].getBasicStatsSketch();
        BasicColStatSketch sk3 = cols[2].getBasicStatsSketch();
        TripleSketch<ITable, BasicColStats, BasicColStats, BasicColStats> tsk =
                new TripleSketch<ITable, BasicColStats, BasicColStats, BasicColStats>(sk1, sk2, sk3);
        this.runSketch(this.table, tsk, request, session);
    }

    @HillviewRpc
    void filterEquality(RpcRequest request, Session session) {
        EqualityFilterDescription info = request.parseArgs(EqualityFilterDescription.class);
        String colName = info.columnDescription.name;
        Object compareValue;

        switch (info.columnDescription.kind) {
            case String:
            case Json:
            case Category:
                compareValue = info.compareValue;
                break;
            case Integer:
                compareValue = Integer.parseInt(info.compareValue);
                break;
            case Date:
            case Double:
            case Duration:
                compareValue = Double.parseDouble(info.compareValue);
                break;
            default:
                throw new RuntimeException("Unhandled column kind " + info.columnDescription.kind);
        }

        EqualityFilter equalityFilter = new EqualityFilter(colName, compareValue, info.complement);
        FilterMap filterMap = new FilterMap(equalityFilter);
        this.runMap(this.table, filterMap, TableTarget::new, request, session);
    }

    @HillviewRpc
    void filterRange(RpcRequest request, Session session) {
        RangeFilterDescription info = request.parseArgs(RangeFilterDescription.class);
        RangeFilter filter = new RangeFilter(info);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, session);
    }

    @HillviewRpc
    void filter2DRange(RpcRequest request, Session session) {
        RangeFilterPair info = request.parseArgs(RangeFilterPair.class);
        Range2DFilter filter = new Range2DFilter(info);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, session);
    }

    static class CorrelationMatrixRequest {
        @Nullable
        String[] columnNames;
    }

    @HillviewRpc
    void correlationMatrix(RpcRequest request, Session session) {
        CorrelationMatrixRequest pcaReq = request.parseArgs(CorrelationMatrixRequest.class);
        List<String> colNames = Arrays.asList(Converters.checkNull(pcaReq.columnNames));
        FullCorrelationSketch sketch = new FullCorrelationSketch(colNames);
        this.runCompleteSketch(this.table, sketch, CorrelationMatrixTarget::new, request, session);
    }

    static class ProjectToEigenVectorsInfo {
        String id = "";
        int numComponents;
    }

    @HillviewRpc
    void projectToEigenVectors(RpcRequest request, Session session) {
        ProjectToEigenVectorsInfo info = request.parseArgs(ProjectToEigenVectorsInfo.class);
        RpcTarget target = RpcObjectManager.instance.getObject(info.id);
        CorrelationMatrixTarget cmt = (CorrelationMatrixTarget) target;
        CorrMatrix cm = cmt.corrMatrix;
        DoubleMatrix[] mats = LinAlg.eigenVectorsVarianceExplained(new DoubleMatrix(cm.getCorrelationMatrix()), info.numComponents);
        DoubleMatrix projectionMatrix = mats[0];
        DoubleMatrix varianceExplained = mats[1];
        List<String> newColNames = new ArrayList<String>();
        for (int i = 0; i < projectionMatrix.rows; i++) {
            int perc = (int) Math.round(varianceExplained.get(i) * 100);
            newColNames.add(String.format("PCA%d (%d%%)", i, perc));
        }
        LinearProjectionMap lpm = new LinearProjectionMap(cm.columnNames, projectionMatrix, newColNames);
        this.runMap(this.table, lpm, TableTarget::new, request, session);
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

    static class HeavyHittersInfo {
        @Nullable
        Schema columns;
        double amount;
    }

    @HillviewRpc
    void heavyHitters(RpcRequest request, Session session) {
        HeavyHittersInfo info = request.parseArgs(HeavyHittersInfo.class);
        Converters.checkNull(info);
        FreqKSketch sk = new FreqKSketch(Converters.checkNull(info.columns), info.amount/100);
        this.runCompleteSketch(this.table, sk, HeavyHittersTarget::new, request, session);
    }

    static class HeavyHittersFilterInfo {
        String hittersId = "";
        @Nullable
        Schema schema;
    }

    public static class TopList implements IJson {
        @Nullable
        NextKList top;
        String heavyHittersId = "";
    }

    private TopList getLists(FreqKList fkList, Schema schema) {
        System.out.printf("Original size: %d\n", fkList.getList().size());
        System.out.printf("Table size: %d, Buckets %d, Cutoff %f\n", fkList.totalRows,
                fkList.maxSize, (double)fkList.totalRows/fkList.maxSize);
        fkList.filter();
        System.out.printf("Filtered size: %d", fkList.getList().size());
        Pair<List<RowSnapshot>, List<Integer>> pair = fkList.getTop();
        TopList tl = new TopList();
        SmallTable tbl = new SmallTable(schema, Converters.checkNull(pair.first));
        tl.top = new NextKList(tbl, Converters.checkNull(pair.second), 0, fkList.totalRows);
        tl.heavyHittersId = Converters.checkNull(new HeavyHittersTarget(fkList).objectId);
        return tl;
    }

    @HillviewRpc
    void checkHeavy(RpcRequest request, Session session) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        RpcTarget target = RpcObjectManager.instance.getObject(hhi.hittersId);
        HeavyHittersTarget hht = (HeavyHittersTarget)target;
        ExactFreqSketch efSketch = new ExactFreqSketch(Converters.checkNull(hhi.schema), hht.heavyHitters);
        this.runCompleteSketch(this.table, efSketch, x -> this.getLists(x, hhi.schema), request, session);
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

    @HillviewRpc
    void hLogLog(RpcRequest request, Session session) {
        String colName = request.parseArgs(String.class);
        HLogLogSketch sketch = new HLogLogSketch(colName);
        this.runSketch(this.table, sketch, request, session);
    }

    static class ConvertColumnInfo {
        String colName = "";
        String newColName = "";
        ContentsKind newKind = ContentsKind.Category;
    }

    @HillviewRpc
    void convertColumnMap(RpcRequest request, Session session) {
        ConvertColumnInfo info = request.parseArgs(ConvertColumnInfo.class);
        ConvertColumnMap map = new ConvertColumnMap(info.colName, info.newColName, info.newKind);
        this.runMap(this.table, map, TableTarget::new, request, session);
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }

    @HillviewRpc
    void zip(RpcRequest request, Session session) {
        String otherId = request.parseArgs(String.class);
        RpcTarget otherObj = RpcObjectManager.instance.getObject(otherId);
        TableTarget otherTable = (TableTarget)otherObj;
        this.runZip(this.table, otherTable.table, TablePairTarget::new, request, session);
    }
}
