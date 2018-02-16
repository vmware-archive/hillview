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

package org.hillview.targets;

import com.google.gson.JsonObject;
import org.hillview.*;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.TripleSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.jsonObjects.Histogram2DArgs;
import org.hillview.jsonObjects.Histogram3DArgs;
import org.hillview.jsonObjects.HistogramArgs;
import org.hillview.maps.*;
import org.hillview.sketches.*;
import org.hillview.table.*;
import org.hillview.table.api.*;
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
    public void getSchema(RpcRequest request, RpcRequestContext context) {
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
    public void getNextK(RpcRequest request, RpcRequestContext context) {
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
    public void uniqueStrings(RpcRequest request, RpcRequestContext context) {
        String[] columnNames = request.parseArgs(String[].class);
        DistinctStringsSketch sk = new DistinctStringsSketch(0, columnNames);
        this.runCompleteSketch(this.table, sk, (e, c)->e, request, context);
    }

    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramArgs info = request.parseArgs(HistogramArgs.class);
        HistogramSketch histo = info.getSketch(false);
        HistogramSketch cdf = info.getSketch(true);
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(cdf, histo);
        this.runSketch(this.table, csk, request, context);
    }

    @HillviewRpc
    public void heatMap(RpcRequest request, RpcRequestContext context) {
        Histogram2DArgs info = request.parseArgs(Histogram2DArgs.class);
        assert info.first != null;
        assert info.second != null;
        HeatMapSketch sk = new HeatMapSketch(
                info.first.getBuckets(info.xBucketCount),
                info.second.getBuckets(info.yBucketCount),
                info.first.getDescription(),
                info.second.getDescription(),
                info.samplingRate, info.seed);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) {
        Histogram2DArgs info = request.parseArgs(Histogram2DArgs.class);
        assert info.first != null;
        assert info.second != null;
        HeatMapSketch sk = new HeatMapSketch(
                info.first.getBuckets(info.xBucketCount),
                info.second.getBuckets(info.yBucketCount),
                info.first.getDescription(),
                info.second.getDescription(),
                info.samplingRate, info.seed);
        IBucketsDescription buckets = info.first.getBuckets(info.cdfBucketCount);
        HistogramSketch cdf = new HistogramSketch(buckets, info.first.getDescription(), info.cdfSamplingRate, info.seed);
        ConcurrentSketch<ITable, HeatMap, Histogram> csk =
                new ConcurrentSketch<ITable, HeatMap, Histogram>(sk, cdf);
        this.runSketch(this.table, csk, request, context);
    }

    @HillviewRpc
    public void heatMap3D(RpcRequest request, RpcRequestContext context) {
        Histogram3DArgs info = request.parseArgs(Histogram3DArgs.class);
        assert info.first != null;
        assert info.second != null;
        assert info.third != null;
        HeatMap3DSketch sk = new HeatMap3DSketch(
                info.first.getBuckets(info.xBucketCount),
                info.second.getBuckets(info.yBucketCount),
                info.third.getBuckets(info.zBucketCount),
                info.first.getDescription(),
                info.second.getDescription(),
                info.third.getDescription(),
                info.samplingRate, info.seed);
        this.runSketch(this.table, sk, request, context);
    }

    static class RangeInfo {
        String columnName = "";
        // The following are only used for categorical columns
        @Nullable
        String[] allNames;
        long seed;

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
            return new BasicColStatSketch(this.getColumn(), 0, 1.0, this.seed);
        }
    }

    @HillviewRpc
    public void range(RpcRequest request, RpcRequestContext context) {
        RangeInfo info = request.parseArgs(RangeInfo.class);
        BasicColStatSketch sk = new BasicColStatSketch(info.getColumn(), 0, 1.0, info.seed);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void range2D(RpcRequest request, RpcRequestContext context) {
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
    public void range3D(RpcRequest request, RpcRequestContext context) {
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
    public void filterEquality(RpcRequest request, RpcRequestContext context) {
        EqualityFilterDescription filter = request.parseArgs(EqualityFilterDescription.class);
        FilterMap filterMap = new FilterMap(filter);
        this.runMap(this.table, filterMap, TableTarget::new, request, context);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, context);
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPair filter = request.parseArgs(RangeFilterPair.class);
        FilterMap fm = new FilterMap(filter);
        this.runMap(this.table, fm, TableTarget::new, request, context);
    }

    static class CorrelationMatrixRequest {
        @SuppressWarnings("NullableProblems")
        String[] columnNames;
        long totalRows;
        long seed;
        boolean toSample;
    }

    @HillviewRpc
    public void correlationMatrix(RpcRequest request, RpcRequestContext context) {
        CorrelationMatrixRequest pcaReq = request.parseArgs(CorrelationMatrixRequest.class);
        String[] colNames = Converters.checkNull(pcaReq.columnNames);
        PCACorrelationSketch pcaSketch;
        if (pcaReq.toSample)
            pcaSketch = new PCACorrelationSketch(colNames, pcaReq.totalRows, pcaReq.seed);
        else
            pcaSketch = new PCACorrelationSketch(colNames);
        this.runCompleteSketch(this.table, pcaSketch, CorrelationMatrixTarget::new, request, context);
    }

    static class ProjectToEigenVectorsInfo {
        String id = "";
        int numComponents;
        String projectionName = "";
    }

    @HillviewRpc
    public void projectToEigenVectors(RpcRequest request, RpcRequestContext context) {
        ProjectToEigenVectorsInfo info = request.parseArgs(ProjectToEigenVectorsInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                CorrelationMatrixTarget cmt = (CorrelationMatrixTarget)rpcTarget;
                CorrMatrix cm = cmt.corrMatrix;
                DoubleMatrix[] mats = LinAlg.eigenVectorsVarianceExplained(new DoubleMatrix(cm.getCorrelationMatrix()),
                        info.numComponents);
                DoubleMatrix projectionMatrix = mats[0];
                DoubleMatrix varianceExplained = mats[1];
                String[] newColNames = new String[projectionMatrix.rows];
                for (int i = 0; i < projectionMatrix.rows; i++) {
                    int perc = (int) Math.round(varianceExplained.get(i) * 100);
                    newColNames[i] = String.format("%s%d (%d%%)", info.projectionName, i, perc);
                }
                LinearProjectionMap lpm = new LinearProjectionMap(cm.columnNames, projectionMatrix, newColNames);
                TableTarget.this.runMap(TableTarget.this.table, lpm, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(info.id), true, observer);
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
    public void sampledControlPoints(RpcRequest request, RpcRequestContext context) {
        SampledControlPoints info = request.parseArgs(SampledControlPoints.class);
        assert info.columnNames != null;
        double samplingRate = ((double) info.numSamples) / info.rowCount;
        RandomSamplingSketch sketch = new RandomSamplingSketch(
                samplingRate, info.seed, info.columnNames, info.allowMissing);
        this.runCompleteSketch(this.table, sketch, (sampled, c) -> {
            sampled = sampled.compress(sampled.getMembershipSet().sample(info.numSamples, info.seed + 1)); // Resample to get the exact number of samples.
            return new ControlPointsTarget(sampled, info.columnNames, c);
        }, request, context);
    }

    static class CatCentroidControlPoints {
        String categoricalColumnName = "";
        @SuppressWarnings("NullableProblems")
        String[] numericalColumnNames;
    }

    @HillviewRpc
    public void categoricalCentroidsControlPoints(RpcRequest request, RpcRequestContext session) {
        CatCentroidControlPoints info = request.parseArgs(CatCentroidControlPoints.class);
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(
                info.categoricalColumnName, info.numericalColumnNames);
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
    public void makeMDSProjection(RpcRequest request, RpcRequestContext context) {
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
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(info.id), true, observer);
    }

    @SuppressWarnings("NullableProblems")
    static class LAMPMapInfo {
        String controlPointsId = "";
        String[] colNames;
        ControlPoints2D newLowDimControlPoints;
        String[] newColNames;
    }

    @HillviewRpc
    public void lampMap(RpcRequest request, RpcRequestContext context) {
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
                LAMPMap map = new LAMPMap(highDimPoints, lowDimPoints, info.colNames, info.newColNames);
                TableTarget.this.runMap(TableTarget.this.table, map, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(info.controlPointsId), true, observer);
    }

    static class QuantileInfo {
        int precision;
        double position;
        long tableSize;
        long seed;
        RecordOrder order = new RecordOrder();
    }

    @HillviewRpc
    public void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(info.order, info.precision, info.tableSize, info.seed);
        BiFunction<SampleList, HillviewComputation, RowSnapshot> getRow = (ql, c) -> ql.getRow(info.position);
        this.runCompleteSketch(this.table, sk, getRow, request, context);
    }

    static class HeavyHittersInfo {
        @SuppressWarnings("NullableProblems")
        Schema columns;
        double amount;
        long totalRows;
        long seed;
    }

    /**
     * This serializes the result of heavyHitterSketch for the front end.
     */
    @SuppressWarnings("NullableProblems")
    public static class TopList implements IJson {
        /**
         * The NextKList stores the fields to display and their counts.
         */
        NextKList top;
        /**
         * The id of the FreqKList object which might be used for further filtering.
         */
        String heavyHittersId;
    }

    /**
     * Post-processing method applied to the result of a heavy hitters sketch before displaying the results. It will
     * discard elements that are too low in (estimated) frequency.
     * @param fkList The list of candidate heavy hitters
     * @param schema The schema of the heavy hitters computation.
     * @param type 0 represents MG, 1 represents SampleHeavyHitters
     * @return A TopList
     */
    private static TopList getLists(FreqKList fkList, Schema schema, int type, HillviewComputation computation) {
        Pair<List<RowSnapshot>, List<Integer>> pair = fkList.getTop(type);
        TopList tl = new TopList();
        SmallTable tbl = new SmallTable(schema, Converters.checkNull(pair.first));
        tl.top = new NextKList(tbl, Converters.checkNull(pair.second), 0, fkList.totalRows);
        tl.heavyHittersId = new HeavyHittersTarget(fkList, computation).getId().toString();
        return tl;
    }

    /**
     * Calls the Misra-Greis (streaming) heavy hitters routine.
     */
    @HillviewRpc
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        HeavyHittersInfo info = request.parseArgs(HeavyHittersInfo.class);
        FreqKSketch sk = new FreqKSketch(info.columns, info.amount/100);
        this.runCompleteSketch(this.table, sk, (x, c) -> TableTarget.getLists(x, info.columns, 0, c),
                request, context);
    }

    /**
     * Calls the Sampling heavy hitters routine.
     */
    @HillviewRpc
    public void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersInfo info = request.parseArgs(HeavyHittersInfo.class);
        SampleHeavyHittersSketch shh = new SampleHeavyHittersSketch(info.columns,
                info.amount/100, info.totalRows, info.seed);
        this.runCompleteSketch(this.table, shh, (x, c) -> TableTarget.getLists(x, info.columns, 2, c),
                request, context);
    }

    static class HeavyHittersFilterInfo {
        String hittersId = "";
        @SuppressWarnings("NullableProblems")
        Schema schema;
    }

    /**
     * Runs the ExactFreqSketch method on a candidate list of heavy hitters.
     */
    @HillviewRpc
    public void checkHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                HeavyHittersTarget hht = (HeavyHittersTarget)rpcTarget;
                ExactFreqSketch efSketch = new ExactFreqSketch(hhi.schema, hht.heavyHitters);
                TableTarget.this.runCompleteSketch(
                        TableTarget.this.table, efSketch, (x, c) -> TableTarget.getLists(x, hhi.schema, 1, c),
                        request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(hhi.hittersId), true, observer);
    }

    /**
     * Creates a table containing only the heavy hitters.
     */
    @HillviewRpc
    public void filterHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                HeavyHittersTarget hht = (HeavyHittersTarget)rpcTarget;
                ITableFilterDescription filter = hht.heavyHitters.heavyFilter(hhi.schema);
                FilterMap fm = new FilterMap(filter);
                TableTarget.this.runMap(TableTarget.this.table, fm, TableTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(hhi.hittersId), true, observer);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        RangeInfo col = request.parseArgs(RangeInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        this.runSketch(this.table, sketch, request, context);
    }

    static class ConvertColumnInfo {
        String colName = "";
        String newColName = "";
        int columnIndex;
        ContentsKind newKind = ContentsKind.Category;
    }

    @HillviewRpc
    public void convertColumnMap(RpcRequest request, RpcRequestContext context) {
        ConvertColumnInfo info = request.parseArgs(ConvertColumnInfo.class);
        ConvertColumnMap map = new ConvertColumnMap(info.colName, info.newColName, info.newKind, info.columnIndex);
        this.runMap(this.table, map, TableTarget::new, request, context);
    }

    @Override
    public String toString() {
        return "TableTarget object=" + super.toString();
    }

    @HillviewRpc
    public void zip(RpcRequest request, RpcRequestContext context) {
        String otherId = request.parseArgs(String.class);
        Observer<RpcTarget> observer = new SingleObserver<RpcTarget>() {
            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                TableTarget otherTable = (TableTarget)rpcTarget;
                TableTarget.this.runZip(
                        TableTarget.this.table, otherTable.table, TablePairTarget::new, request, context);
            }
        };
        RpcObjectManager.instance.retrieveTarget(new RpcTarget.Id(otherId), true, observer);
    }

    @SuppressWarnings("NullableProblems")
    static class CreateColumnInfo {
        String jsFunction = "";
        Schema schema;
        String outputColumn;
        ContentsKind outputKind = ContentsKind.Category;
    }

    @HillviewRpc
    public void createColumn(RpcRequest request, RpcRequestContext context) {
        CreateColumnInfo info = request.parseArgs(CreateColumnInfo.class);
        ColumnDescription desc = new ColumnDescription(info.outputColumn, info.outputKind, true);
        CreateColumnJSMap map = new CreateColumnJSMap(info.jsFunction, info.schema, desc);
        this.runMap(this.table, map, TableTarget::new, request, context);
    }
}
