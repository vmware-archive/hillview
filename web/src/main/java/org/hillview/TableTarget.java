/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import com.google.gson.JsonObject;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.TripleSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.ConvertColumnMap;
import org.hillview.maps.FilterMap;
import org.hillview.maps.LAMPMap;
import org.hillview.maps.LinearProjectionMap;
import org.hillview.sketches.*;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.ColPair;
import org.hillview.table.columns.ColTriple;
import org.hillview.table.filters.EqualityFilterDescription;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.filters.RangeFilterPair;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.LinAlg;
import org.hillview.utils.Point2D;
import org.jblas.DoubleMatrix;
import rx.Observer;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.List;
import java.util.function.BiFunction;

/**
 * This is the most important RpcTarget, representing a remote table.
 * Almost all operations are triggered from this object.
 */
public final class TableTarget extends RpcTarget {
    private final IDataSet<ITable> table;
    TableTarget(IDataSet<ITable> table, HillviewComputation computation) {
        super(computation);
        this.table = table;
        this.registerObject();
    }

    @HillviewRpc
    void getSchema(RpcRequest request, RpcRequestContext context) {
        SummarySketch ss = new SummarySketch();
        this.runSketch(this.table, ss, request, context);
    }

    static class NextKArgs {
        final RecordOrder order = new RecordOrder();
        @Nullable
        Object[] firstRow;
        int rowsOnScreen;
    }

    @HillviewRpc
    void getNextK(RpcRequest request, RpcRequestContext context) {
        NextKArgs nextKArgs = request.parseArgs(NextKArgs.class);
        RowSnapshot rs = null;
        if (nextKArgs.firstRow != null) {
            Schema schema = nextKArgs.order.toSchema();
            rs = RowSnapshot.parse(schema, nextKArgs.firstRow);
        }
        NextKSketch nk = new NextKSketch(nextKArgs.order, rs, nextKArgs.rowsOnScreen);
        this.runSketch(this.table, nk, request, context);
    }

    @HillviewRpc
    void uniqueStrings(RpcRequest request, RpcRequestContext context) {
        String[] columnNames = request.parseArgs(String[].class);
        DistinctStringsSketch sk = new DistinctStringsSketch(0, columnNames);
        this.runCompleteSketch(this.table, sk, (e, c)->e, request, context);
    }

    @HillviewRpc
    void histogram(RpcRequest request, RpcRequestContext context) {
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
        this.runSketch(this.table, csk, request, context);
    }

    @HillviewRpc
    void heatMap(RpcRequest request, RpcRequestContext context) {
        ColPair info = request.parseArgs(ColPair.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        // We expect the sampling rate to be the same for both columns
        HeatMapSketch sk = new HeatMapSketch(h1.buckets, h2.buckets, h1.column, h2.column, info.first.samplingRate);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    void heatMap3D(RpcRequest request, RpcRequestContext context) {
        ColTriple info = request.parseArgs(ColTriple.class);
        ColumnAndRange.HistogramParts h1 = Converters.checkNull(info.first).prepare();
        ColumnAndRange.HistogramParts h2 = Converters.checkNull(info.second).prepare();
        ColumnAndRange.HistogramParts h3 = Converters.checkNull(info.third).prepare();
        HeatMap3DSketch sk = new HeatMap3DSketch(h1.buckets, h2.buckets, h3.buckets,
                h1.column, h2.column, h3.column);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    void histogram2D(RpcRequest request, RpcRequestContext context) {
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
        this.runSketch(this.table, csk, request, context);
    }

    static class RangeInfo {
        String columnName = "";
        // The following are only used for categorical columns
        @Nullable
        String[] allNames;

        ColumnAndConverterDescription getColumn() {
            IStringConverterDescription converter;
            if (this.allNames != null)
                converter = new SortedStringsConverterDescription(this.allNames);
            else
                // only used if the column has Strings
                converter = new RadixConverter();
            return new ColumnAndConverterDescription(this.columnName, converter);
        }

        BasicColStatSketch getBasicStatsSketch() {
            return new BasicColStatSketch(this.getColumn(), 0, 1.0);
        }
    }

    @HillviewRpc
    void range(RpcRequest request, RpcRequestContext context) {
        RangeInfo info = request.parseArgs(RangeInfo.class);
        BasicColStatSketch sk = new BasicColStatSketch(info.getColumn(), 0, 1.0);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    void range2D(RpcRequest request, RpcRequestContext context) {
        RangeInfo[] cols = request.parseArgs(RangeInfo[].class);
        if (cols.length != 2)
            throw new RuntimeException("Expected 2 RangeInfo objects, got " + cols.length);
        BasicColStatSketch sk1 = cols[0].getBasicStatsSketch();
        BasicColStatSketch sk2 = cols[1].getBasicStatsSketch();
        ConcurrentSketch<ITable, BasicColStats, BasicColStats> csk =
                new ConcurrentSketch<ITable, BasicColStats, BasicColStats>(sk1, sk2);
        this.runSketch(this.table, csk, request, context);
    }

    @HillviewRpc
    void range3D(RpcRequest request, RpcRequestContext context) {
        RangeInfo[] cols = request.parseArgs(RangeInfo[].class);
        if (cols.length != 3)
            throw new RuntimeException("Expected 3 RangeInfo objects, got " + cols.length);
        BasicColStatSketch sk1 = cols[0].getBasicStatsSketch();
        BasicColStatSketch sk2 = cols[1].getBasicStatsSketch();
        BasicColStatSketch sk3 = cols[2].getBasicStatsSketch();
        TripleSketch<ITable, BasicColStats, BasicColStats, BasicColStats> tsk =
                new TripleSketch<ITable, BasicColStats, BasicColStats, BasicColStats>(sk1, sk2, sk3);
        this.runSketch(this.table, tsk, request, context);
    }

    @HillviewRpc
    void filterEquality(RpcRequest request, RpcRequestContext context) {
        EqualityFilterDescription filter = request.parseArgs(EqualityFilterDescription.class);
        FilterMap filterMap = new FilterMap(filter);
        this.runMap(this.table, filterMap, TableTarget::new, request, context);
    }

    @HillviewRpc
    void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, context);
    }

    @HillviewRpc
    void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPair filter = request.parseArgs(RangeFilterPair.class);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, context);
    }

    static class CorrelationMatrixRequest {
        @Nullable
        String[] columnNames;
    }

    @HillviewRpc
    void correlationMatrix(RpcRequest request, RpcRequestContext context) {
        CorrelationMatrixRequest pcaReq = request.parseArgs(CorrelationMatrixRequest.class);
        String[] colNames = Converters.checkNull(pcaReq.columnNames);
        FullCorrelationSketch sketch = new FullCorrelationSketch(colNames);
        this.runCompleteSketch(this.table, sketch, CorrelationMatrixTarget::new, request, context);
    }

    static class ProjectToEigenVectorsInfo {
        String id = "";
        int numComponents;
    }

    @HillviewRpc
    void projectToEigenVectors(RpcRequest request, RpcRequestContext context) {
        ProjectToEigenVectorsInfo info = request.parseArgs(ProjectToEigenVectorsInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                CorrelationMatrixTarget cmt = (CorrelationMatrixTarget)rpcTarget;
                CorrMatrix cm = cmt.corrMatrix;
                DoubleMatrix[] mats = LinAlg.eigenVectorsVarianceExplained(new DoubleMatrix(cm.getCorrelationMatrix()), info.numComponents);
                DoubleMatrix projectionMatrix = mats[0];
                DoubleMatrix varianceExplained = mats[1];
                String[] newColNames = new String[projectionMatrix.rows];
                for (int i = 0; i < projectionMatrix.rows; i++) {
                    int perc = (int) Math.round(varianceExplained.get(i) * 100);
                    newColNames[i] = String.format("PCA%d (%d%%)", i, perc);
                }
                LinearProjectionMap lpm = new LinearProjectionMap(cm.columnNames, projectionMatrix, newColNames);
                TableTarget.this.runMap(TableTarget.this.table, lpm, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(info.id, true, observer);
    }

    static class SampledControlPoints {
        long rowCount;
        int numSamples;
        long seed;
        boolean allowMissing;
        @Nullable
        String[] columnNames;
    }

    @HillviewRpc
    void sampledControlPoints(RpcRequest request, RpcRequestContext context) {
        SampledControlPoints info = request.parseArgs(SampledControlPoints.class);
        double samplingRate = ((double) info.numSamples) / info.rowCount;
        RandomSamplingSketch sketch = new RandomSamplingSketch(
                samplingRate, info.seed, Converters.checkNull(info.columnNames), info.allowMissing);
        this.runCompleteSketch(this.table, sketch, (sampled, c) -> {
            sampled = sampled.compress(sampled.getMembershipSet().sample(info.numSamples, info.seed + 1)); // Resample to get the exact number of samples.
            return new ControlPointsTarget(sampled, info.columnNames, c);
        }, request, context);
    }

    static class CatCentroidControlPoints {
        String categoricalColumnName = "";
        @Nullable
        String[] numericalColumnNames;
    }

    @HillviewRpc
    void categoricalCentroidsControlPoints(RpcRequest request, RpcRequestContext session) {
        CatCentroidControlPoints info = request.parseArgs(CatCentroidControlPoints.class);
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(
                Converters.checkNull(info.categoricalColumnName),
                Converters.checkNull(info.numericalColumnNames));
        this.runCompleteSketch(this.table, sketch, ControlPointsTarget::new, request, session);
    }

    static class MakeMDSProjection {
        String id = "";
        int seed;
    }

    static class ControlPoints2D implements IJson {
        Point2D[] points;
        ControlPoints2D(Point2D[] points) {
            this.points = points;
        }
    }

    @HillviewRpc
    void makeMDSProjection(RpcRequest request, RpcRequestContext context) {
        MakeMDSProjection info = request.parseArgs(MakeMDSProjection.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                ControlPointsTarget controlPointsTarget = (ControlPointsTarget)rpcTarget;
                ControlPoints2D controlPoints2D = controlPointsTarget.mds(info.seed);
                Session session = context.getSessionIfOpen();
                if (session == null)
                    return;

                JsonObject json = new JsonObject();
                json.addProperty("done", 1.0);
                json.add("data", controlPoints2D.toJsonTree());
                RpcReply reply = request.createReply(json);
                reply.send(session);
                request.syncCloseSession(session);
            }
        };
        RpcObjectManager.instance.retrieveTarget(info.id, true, observer);
    }

    static class LAMPMapInfo {
        String controlPointsId = "";
        @Nullable
        String[] colNames;
        @Nullable
        ControlPoints2D newLowDimControlPoints;
        @Nullable
        String[] newColNames;
    }

    @HillviewRpc
    void lampMap(RpcRequest request, RpcRequestContext context) {
        LAMPMapInfo info = request.parseArgs(LAMPMapInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                ControlPointsTarget controlPointsTarget = (ControlPointsTarget)rpcTarget;
                DoubleMatrix highDimPoints = controlPointsTarget.highDimData;
                ControlPoints2D newControlPoints = Converters.checkNull(info.newLowDimControlPoints);
                DoubleMatrix lowDimPoints = new DoubleMatrix(newControlPoints.points.length, 2);
                for (int i = 0; i < newControlPoints.points.length; i++) {
                    lowDimPoints.put(i, 0, newControlPoints.points[i].x);
                    lowDimPoints.put(i, 1, newControlPoints.points[i].y);
                }
                lowDimPoints.print();
                LAMPMap map = new LAMPMap(highDimPoints, lowDimPoints,
                        Converters.checkNull(info.colNames), Converters.checkNull(info.newColNames));
                TableTarget.this.runMap(TableTarget.this.table, map, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(info.controlPointsId, true, observer);
    }

    static class QuantileInfo {
        int precision;
        double position;
        long tableSize;
        RecordOrder order = new RecordOrder();
    }

    @HillviewRpc
    void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(info.order, info.precision, info.tableSize);
        BiFunction<SampleList, HillviewComputation, RowSnapshot> getRow = (ql, c) -> ql.getRow(info.position);
        this.runCompleteSketch(this.table, sk, getRow, request, context);
    }

    static class HeavyHittersInfo {
        @Nullable
        Schema columns;
        double amount;
    }

    @HillviewRpc
    void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersInfo info = request.parseArgs(HeavyHittersInfo.class);
        Converters.checkNull(info);
        FreqKSketch sk = new FreqKSketch(Converters.checkNull(info.columns), info.amount/100);
        this.runCompleteSketch(this.table, sk, (x, c) -> TableTarget.getLists(x, info.columns, true, c),
                request, context);
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

    private static TopList getLists(FreqKList fkList, Schema schema, boolean isMG, HillviewComputation computation) {
        fkList.filter(isMG);
        Pair<List<RowSnapshot>, List<Integer>> pair = fkList.getTop();
        TopList tl = new TopList();
        SmallTable tbl = new SmallTable(schema, Converters.checkNull(pair.first));
        tl.top = new NextKList(tbl, Converters.checkNull(pair.second), 0, fkList.totalRows);
        tl.heavyHittersId = Converters.checkNull(new HeavyHittersTarget(fkList, computation).objectId);
        return tl;
    }

    @HillviewRpc
    void checkHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                HeavyHittersTarget hht = (HeavyHittersTarget)rpcTarget;
                ExactFreqSketch efSketch = new ExactFreqSketch(Converters.checkNull(hhi.schema), hht.heavyHitters);
                TableTarget.this.runCompleteSketch(
                        TableTarget.this.table, efSketch, (x, c) -> TableTarget.getLists(x, hhi.schema, false, c),
                        request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(hhi.hittersId, true, observer);
    }

    @HillviewRpc
    void filterHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                HeavyHittersTarget hht = (HeavyHittersTarget)rpcTarget;
                ITableFilterDescription filter = hht.heavyHitters.heavyFilter(Converters.checkNull(hhi.schema));
                FilterMap fm = new FilterMap(filter);
                TableTarget.this.runMap(TableTarget.this.table, fm, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(hhi.hittersId, true, observer);
    }

    @HillviewRpc
    void hLogLog(RpcRequest request, RpcRequestContext context) {
        String colName = request.parseArgs(String.class);
        HLogLogSketch sketch = new HLogLogSketch(colName);
        this.runSketch(this.table, sketch, request, context);
    }

    static class ConvertColumnInfo {
        String colName = "";
        String newColName = "";
        ContentsKind newKind = ContentsKind.Category;
    }

    @HillviewRpc
    void convertColumnMap(RpcRequest request, RpcRequestContext context) {
        ConvertColumnInfo info = request.parseArgs(ConvertColumnInfo.class);
        ConvertColumnMap map = new ConvertColumnMap(info.colName, info.newColName, info.newKind);
        this.runMap(this.table, map, TableTarget::new, request, context);
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }

    @HillviewRpc
    void zip(RpcRequest request, RpcRequestContext context) {
        String otherId = request.parseArgs(String.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                TableTarget otherTable = (TableTarget)rpcTarget;
                TableTarget.this.runZip(
                        TableTarget.this.table, otherTable.table, TablePairTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(otherId, true, observer);
    }
}
