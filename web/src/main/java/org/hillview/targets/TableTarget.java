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
import org.hillview.dataStructures.*;
import org.hillview.dataset.api.*;
import org.hillview.geo.PolygonSet;
import org.hillview.maps.*;
import org.hillview.sketches.*;
import org.hillview.sketches.highorder.*;
import org.hillview.sketches.results.*;
import org.hillview.table.*;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilterDescription;
import org.hillview.table.filters.*;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.*;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * This is the most important RpcTarget, representing a remote table.
 * Almost all operations are triggered from this object.
 */
@SuppressWarnings("CanBeFinal")
public class TableTarget extends TableRpcTarget {
    static final long serialVersionUID = 1;

    TableTarget(IDataSet<ITable> table, HillviewComputation computation, @Nullable String metadataDirectory) {
        super(computation, metadataDirectory);
        this.setTable(table);
        this.registerObject();
    }

    static class NextKArgs {
        RecordOrder order = new RecordOrder();
        @Nullable
        Object[] firstRow;
        @Nullable
        String[] columnsNoValue;
        int rowsOnScreen;
        @Nullable
        AggregateDescription[] aggregates;
    }

    @Nullable
    static RowSnapshot asRowSnapshot(
            @Nullable Object[] data, RecordOrder order, @Nullable String[] columnsNoValue) {
        if (data == null) return null;
        Schema schema = order.toSchema();
        return RowSnapshot.parseJson(schema, data, columnsNoValue);
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        NextKArgs nextKArgs = request.parseArgs(NextKArgs.class);
        RowSnapshot rs = TableTarget.asRowSnapshot(
                nextKArgs.firstRow, nextKArgs.order, nextKArgs.columnsNoValue);
        NextKSketch nk = new NextKSketch(nextKArgs.order, AggregateDescription.getAggregates(nextKArgs.aggregates),
                rs, nextKArgs.rowsOnScreen);
        NextKSketchAggregate nka = new NextKSketchAggregate(nk, nextKArgs.aggregates);
        this.runSketch(this.table, nka, request, context);
    }

    static class LogFragmentArgs {
        Schema schema = new Schema();
        @Nullable
        Object[] row;
        Schema rowSchema = new Schema();
        int count;
    }

    @HillviewRpc
    public void getLogFragment(RpcRequest request, RpcRequestContext context) {
        LogFragmentArgs args = request.parseArgs(LogFragmentArgs.class);
        RowSnapshot row = null;
        if (args.row != null)
            row = RowSnapshot.parseJson(args.rowSchema, args.row, null);
        LogFragmentSketch lfs = new LogFragmentSketch(
                args.schema, row, args.rowSchema, args.count);
        this.runCompleteSketch(this.table, lfs, request, context);
    }

    @HillviewRpc
    public void prune(RpcRequest request, RpcRequestContext context) {
        this.runPrune(this.table, new EmptyTableMap(), (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void basicColStats(RpcRequest request, RpcRequestContext context) {
        String[] args = request.parseArgs(String[].class);
        BasicColStatSketch sk = new BasicColStatSketch(args, 2);
        PostProcessedSketch<ITable, JsonList<BasicColStats>, JsonList<BasicColStats>> post =
                sk.andThen(stats -> {
                    for (BasicColStats s : Converters.checkNull(stats)) {
                        // We mutate in place; this is safe in the root node.
                        if (s.moments.length > 1)
                            // the value should never be negative, but you can't trust FP
                            s.moments[1] = Math.sqrt(Math.max(0, s.moments[1] - s.moments[0] * s.moments[0]));
                    }
                    return stats;
                });
        // If the view has many columns sending partial results to the
        // UI overwhelms the browser, so we only send the final result.
        this.runCompleteSketch(this.table, post, request, context);
    }

    static class ContainsArgs {
        RecordOrder order = new RecordOrder();
        @Nullable
        Object[] row;
    }

    @HillviewRpc
    public void contains(RpcRequest request, RpcRequestContext context) {
        ContainsArgs args = request.parseArgs(ContainsArgs.class);
        RowSnapshot row = TableTarget.asRowSnapshot(args.row, args.order, null);
        ContainsMap map = new ContainsMap(args.order.toSchema(), Converters.checkNull(row));
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    static class FindArgs {
        @Nullable
        RecordOrder order;
        @Nullable
        Object[] topRow;
        @Nullable
        StringFilterDescription stringFilterDescription;
        boolean excludeTopRow;
        boolean next;
    }

    @HillviewRpc
    public void find(RpcRequest request, RpcRequestContext context) {
        FindArgs args = request.parseArgs(FindArgs.class);
        Converters.checkNull(args.order);
        Converters.checkNull(args.stringFilterDescription);
        RowSnapshot rs = TableTarget.asRowSnapshot(args.topRow, args.order, null);
        FindSketch sk = new FindSketch(args.stringFilterDescription, rs, args.order,
                args.excludeTopRow, args.next);
        this.runCompleteSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void getGeo(RpcRequest request, RpcRequestContext context) throws IOException {
        ColumnDescription desc = request.parseArgs(ColumnDescription.class);
        @Nullable GeoFileInformation geoInfo = this.getGeoColumnInformation(desc.name);
        if (geoInfo == null)
            throw new RuntimeException("No geographic data found for column " + desc.name);
        PolygonSet ps = new PolygonSet(geoInfo.geoFile);
        JsonInString result = geoInfo.createJSON(ps);
        this.returnResult(result, request, context);
    }

    static class SaveAsArgs {
        String fileKind = "";
        String folder = "";
        @Nullable
        Schema schema;
        // Rename map encoded as an array
        @Nullable
        String[] renameMap;
    }

    @HillviewRpc
    public void saveAs(RpcRequest request, RpcRequestContext context) {
        SaveAsArgs args = request.parseArgs(SaveAsArgs.class);
        if (args.fileKind.equals("db")) {
            // TODO: currently this is hardwired for greenplum
            args.folder = Paths.get(Configuration.instance.getGreenplumDumpDirectory(), args.folder).toString();
        }
        SaveAsFileSketch sk = new SaveAsFileSketch(
                args.fileKind, args.folder, args.schema,
                Utilities.arrayToMap(args.renameMap), true);
        this.runCompleteSketch(this.table, sk, request, context);
    }

    static class QuantilesVectorInfo extends HistogramInfo {
        long seed;
        int quantileCount;
        /**
         * Column whose quantiles are being computed.
         */
        String quantilesColumn = "";
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getQuantilesVector(RpcRequest request, RpcRequestContext context) {
        QuantilesVectorInfo info = request.parseArgs(QuantilesVectorInfo.class);
        IHistogramBuckets buckets = info.getBuckets();
        int samplesRequired = 10 * info.quantileCount * info.quantileCount;
        HistogramQuantilesSketch qvs = new HistogramQuantilesSketch(
                info.quantilesColumn, samplesRequired, info.seed, buckets);
        PostProcessedSketch<ITable, Groups<SampleSet>, JsonGroups<SampleSet>> qvr =
                qvs.andThen(g -> g.toSerializable(v -> v.quantiles(info.quantileCount)));
        this.runSketch(this.table, qvr, request, context);
    }

    static class QuantilesMatrixInfo extends QuantilesVectorInfo {
        @SuppressWarnings("NotNullFieldNotInitialized")
        HistogramInfo groupColumn;
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getQuantilesMatrix(RpcRequest request, RpcRequestContext context) {
        QuantilesMatrixInfo info = request.parseArgs(QuantilesMatrixInfo.class);
        IHistogramBuckets gBuckets = info.groupColumn.getBuckets();
        IHistogramBuckets xBuckets = info.getBuckets();

        final int samplesRequired = 10 * info.quantileCount * info.quantileCount;
        Histogram2DQuantilesSketch qvs = new Histogram2DQuantilesSketch(info.quantilesColumn,
                samplesRequired, info.seed, xBuckets, gBuckets);
        PostProcessedSketch<ITable, Groups<Groups<SampleSet>>, JsonGroups<JsonGroups<SampleSet>>> qvr =
                qvs.andThen(g -> g.toSerializable(q -> q.toSerializable(i -> i.quantiles(info.quantileCount))));
        this.runSketch(this.table, qvr, request, context);
    }

    // The following functions return lists with subclasses of BucketsInfo: either
    // StringBucketBoundaries or DataRange.

    // This function manipulates arrays to make it more homogeneous with the other two.
    @SuppressWarnings("unused")
    @HillviewRpc
    public void getDataQuantiles(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        JsonList<PostProcessedSketch<ITable, BucketsInfo, BucketsInfo>> l =
                Linq.mapToList(args, QuantilesArgs::getPostSketch);
        MultiPostprocessedSketch<ITable, BucketsInfo, BucketsInfo> multi =
                new MultiPostprocessedSketch<ITable, BucketsInfo, BucketsInfo>(l);
        this.runCompleteSketch(this.table, multi, request, context);
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void histogramAndCDF(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        TableSketch<Groups<Count>> sk = info.getSketch(0); // Histogram
        TableSketch<Groups<Count>> cdf = info.getSketch(1); // CDF: also histogram but at finer granularity
        ConcurrentSketch<ITable, Groups<Count>, Groups<Count>> csk =
                new ConcurrentSketch<>(sk, cdf);
        DataWithCDFSketch<Groups<Count>> post = new DataWithCDFSketch<Groups<Count>>(csk);
        // Add the confidence intervals as null
        this.runSketch(this.table, post.andThen(p -> new Two<>(new Two<>(p.first), new Two<>(p.second))), request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        Histogram2DSketch sk = new Histogram2DSketch(
                info.getBuckets(1),
                info.getBuckets(0));
        this.runSketch(this.table, sk.andThen(
                r -> new Two<>(r.toSerializable(s -> s.toSerializable(c -> c)), null)), request, context);
    }

    static class HeatmapRequestInfo extends HistogramRequestInfo {
        @SuppressWarnings("NotNullFieldNotInitialized")
        Schema schema;
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        HeatmapRequestInfo info = request.parseArgs(HeatmapRequestInfo.class);
        assert info.size() == 2;
        HeatmapSketch sk = new HeatmapSketch(
                info.schema,
                info.getBuckets(1),
                info.getBuckets(0));
        this.runSketch(this.table, sk.andThen(
                r -> new Pair<>(
                        r.toSerializable(g -> g.toSerializable(CountAndSingleton::getCount)),
                        r.toSerializable(g -> g.toSerializable(c -> c.row))
                )), request, context);
    }

    @HillviewRpc
    public void correlationHeatmaps(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        IHistogramBuckets[] buckets = Linq.map(info.histos, HistogramInfo::getBuckets, IHistogramBuckets.class);
        CorrelationSketch sk = new CorrelationSketch(buckets, info.samplingRate, info.seed);
        this.runSketch(this.table, sk, request, context);
    }

    @HillviewRpc
    public void histogram2DAndCDF(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 3;
        TableSketch<Groups<Groups<Count>>> sk = new Histogram2DSketch(
                info.getBuckets(1),
                info.getBuckets(0)).sampled(info.samplingRate, info.seed);
        TableSketch<Groups<Count>> cdf = info.getSketch(2);
        ConcurrentSketch<ITable, Groups<Groups<Count>>, Groups<Count>> csk =
                new ConcurrentSketch<ITable, Groups<Groups<Count>>, Groups<Count>>(sk, cdf);
        DataWithCDFSketch<Groups<Groups<Count>>> dwc = new DataWithCDFSketch<Groups<Groups<Count>>>(csk);
        this.runSketch(this.table, dwc, request, context);
    }

    @HillviewRpc
    public void histogram3D(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 3;
        Histogram3DSketch sk = new Histogram3DSketch(
                info.getBuckets(2),
                info.getBuckets(1),
                info.getBuckets(0));
        TableSketch<Groups<Groups<Groups<Count>>>> sts = sk.sampled(info.samplingRate, info.seed);
        this.runSketch(this.table, sts.andThen(res -> res.toSerializable(
                r -> r.toSerializable(s -> s.toSerializable(c -> c)))), request, context);
    }

    private void runFilter(
            ITableFilterDescription filter, RpcRequest request, RpcRequestContext context) {
        FilterMap filterMap = new FilterMap(filter);
        this.runMap(this.table, filterMap, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    static class RowFilterDescription {
        RecordOrder order = new RecordOrder();
        @Nullable
        Object[] data;
        String comparison = "";

        ITableFilterDescription getFilter() {
            RowSnapshot row = TableTarget.asRowSnapshot(
                    this.data, this.order, null);
            return new RowComparisonFilterDescription(
                    Converters.checkNull(row), order, comparison);
        }
    }

    @HillviewRpc
    public void filterOnRow(RpcRequest request, RpcRequestContext context) {
        RowFilterDescription desc = request.parseArgs(RowFilterDescription.class);
        this.runFilter(desc.getFilter(), request, context);
    }

    @HillviewRpc
    public void filterColumns(RpcRequest request, RpcRequestContext context) {
        StringColumnsFilterDescription filter = request.parseArgs(StringColumnsFilterDescription.class);
        this.runFilter(filter, request, context);
    }

    @HillviewRpc
    public void filterColumn(RpcRequest request, RpcRequestContext context) {
        StringColumnFilterDescription filter = request.parseArgs(StringColumnFilterDescription.class);
        this.runFilter(filter, request, context);
    }

    @HillviewRpc
    public void filterComparison(RpcRequest request, RpcRequestContext context) {
        ComparisonFilterDescription filter = request.parseArgs(ComparisonFilterDescription.class);
        this.runFilter(filter, request, context);
    }

    @HillviewRpc
    public void filterRanges(RpcRequest request, RpcRequestContext context) {
        RangeFilterArrayDescription filter = request.parseArgs(RangeFilterArrayDescription.class);
        this.runFilter(filter, request, context);
    }

    @SuppressWarnings("NotNullFieldNotInitialized")
    static class CorrelationMatrixRequest {
        String[] columnNames;
        long totalRows;
        long seed;
        boolean toSample;
    }

    private PCACorrelationSketch getPCASketch(RpcRequest request) {
        CorrelationMatrixRequest pcaReq = request.parseArgs(CorrelationMatrixRequest.class);
        String[] colNames = Converters.checkNull(pcaReq.columnNames);
        PCACorrelationSketch pcaSketch;
        if (pcaReq.toSample)
            pcaSketch = new PCACorrelationSketch(colNames, pcaReq.totalRows, pcaReq.seed);
        else
            pcaSketch = new PCACorrelationSketch(colNames);
        return pcaSketch;
    }

    @HillviewRpc
    public void correlationMatrix(RpcRequest request, RpcRequestContext context) {
        PCACorrelationSketch pcaSketch = this.getPCASketch(request);
        PostProcessedSketch<ITable, CorrMatrix, CorrelationMatrixTarget> post =
                pcaSketch.andThen(r -> new CorrelationMatrixTarget(r, context.getComputation(request)));
        this.runCompleteSketch(this.table, post, request, context);
    }

    @HillviewRpc
    public void spectrum(RpcRequest request, RpcRequestContext context) {
        PCACorrelationSketch pcaSketch = this.getPCASketch(request);
        PostProcessedSketch<ITable, CorrMatrix, CorrelationMatrixTarget.EigenVal> post =
                pcaSketch.andThen(result -> {
                    HillviewComputation computation = context.getComputation(request);
                    CorrelationMatrixTarget target = new CorrelationMatrixTarget(result, computation);
                    return target.eigenValues();
                });
        this.runCompleteSketch(this.table, post, request, context);
    }

    static class ProjectToEigenVectorsInfo {
        String id = "";
        int numComponents;
        String projectionName = "";
    }

    @HillviewRpc
    public void projectToEigenVectors(RpcRequest request, RpcRequestContext context) {
        ProjectToEigenVectorsInfo info = request.parseArgs(ProjectToEigenVectorsInfo.class);
        RpcObjectManager.instance.when(info.id, target -> {
            CorrelationMatrixTarget cmt = target.to(CorrelationMatrixTarget.class);
            CorrMatrix cm = cmt.corrMatrix;
            DoubleMatrix[] mats = LinAlg.eigenVectorsVarianceExplained(new DoubleMatrix(cm.getCorrelationMatrix()),
                    info.numComponents);
            DoubleMatrix projectionMatrix = mats[0];
            DoubleMatrix varianceExplained = mats[1];
            String[] newColNames = new String[projectionMatrix.rows];
            for (int i = 0; i < projectionMatrix.rows; i++) {
                int perc = Converters.toInt(Math.round(varianceExplained.get(i) * 100));
                newColNames[i] = String.format("%s%d (%d%%)", info.projectionName, i, perc);
            }
            LinearProjectionMap lpm = new LinearProjectionMap(cm.columnNames, projectionMatrix, newColNames);
            TableTarget.this.runMap(TableTarget.this.table, lpm, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
        });
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
        PostProcessedSketch<ITable, SmallTable, RpcTarget> post = sketch.andThen(sampled -> {
            IMembershipSet sampleRows = sampled.getMembershipSet().sample(
                    info.numSamples, info.seed + 1);
            sampled = sampled.compress(sampleRows);
            return new ControlPointsTarget(sampled, info.columnNames, context.getComputation(request));
        });
        this.runCompleteSketch(this.table, post, request, context);
    }

    @HillviewRpc
    public void categoricalCentroidsControlPoints(RpcRequest request, RpcRequestContext context) {
        CategoryCentroidsSketch.Info info = request.parseArgs(CategoryCentroidsSketch.Info.class);
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(info);
        PostProcessedSketch<ITable, Centroids<String>, RpcTarget> post =
                sketch.andThen(r -> new ControlPointsTarget(r, context.getComputation(request)));
        this.runCompleteSketch(this.table, post, request, context);
    }

    static class MakeMDSProjection {
        String id = "";
        int seed;
    }

    static class ControlPoints2D implements IJson {
        static final long serialVersionUID = 1;

        Point2D[] points;
        ControlPoints2D(Point2D[] points) {
            this.points = points;
        }
    }

    @HillviewRpc
    public void makeMDSProjection(RpcRequest request, RpcRequestContext context) {
        MakeMDSProjection info = request.parseArgs(MakeMDSProjection.class);
        RpcObjectManager.instance.when(info.id, rpcTarget -> {
            ControlPointsTarget controlPointsTarget = rpcTarget.to(ControlPointsTarget.class);
            ControlPoints2D controlPoints2D = controlPointsTarget.mds(info.seed);
            Session session = context.getSessionIfOpen();
            if (session == null)
                return;

            JsonObject json = new JsonObject();
            json.addProperty("done", 1.0);
            json.add("data", controlPoints2D.toJsonTree());
            RpcReply reply = request.createReply(json);
            RpcServer.sendReply(reply, session);
            request.syncCloseSession(session);
        });
    }

    @SuppressWarnings("NotNullFieldNotInitialized")
    static class LAMPMapInfo {
        String controlPointsId = "";
        String[] colNames;
        ControlPoints2D newLowDimControlPoints;
        String[] newColNames;
    }

    @HillviewRpc
    public void lampMap(RpcRequest request, RpcRequestContext context) {
        LAMPMapInfo info = request.parseArgs(LAMPMapInfo.class);
        RpcObjectManager.instance.when(info.controlPointsId, rpcTarget -> {
            ControlPointsTarget controlPointsTarget = rpcTarget.to(ControlPointsTarget.class);
            DoubleMatrix highDimPoints = controlPointsTarget.highDimData;
            ControlPoints2D newControlPoints = Converters.checkNull(info.newLowDimControlPoints);
            DoubleMatrix lowDimPoints = new DoubleMatrix(newControlPoints.points.length, 2);
            for (int i = 0; i < newControlPoints.points.length; i++) {
                lowDimPoints.put(i, 0, newControlPoints.points[i].x);
                lowDimPoints.put(i, 1, newControlPoints.points[i].y);
            }
            lowDimPoints.print();
            LAMPMap map = new LAMPMap(highDimPoints, lowDimPoints, info.colNames, info.newColNames);
            TableTarget.this.runMap(TableTarget.this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
        });
    }


    @HillviewRpc
    public void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(
                info.order, info.precision, info.tableSize, info.seed);
        PostProcessedSketch<ITable, SampleList, RowSnapshot> getQuantile =
                sk.andThen(t -> t.getRow(info.position));
        this.runCompleteSketch(this.table, getQuantile, request, context);
    }

    /**
     * Calls the Misra-Gries (streaming) heavy hitters routine.
     */
    @HillviewRpc
    public void heavyHittersMG(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        MGFreqKSketch sk = new MGFreqKSketch(info.columns, info.amount/100);
        PostProcessedSketch<ITable, FreqKListMG, TopList> post =
                sk.andThen(result -> {
                HillviewComputation computation = context.getComputation(request);
                HeavyHittersTarget target = new HeavyHittersTarget(result, computation);
                NextKList top = result.getTop(info.columns);
                return new TopList(top, target.getId().toString());
            });
        this.runCompleteSketch(this.table, post, request, context);
    }

    /**
     * Calls the Sampling heavy hitters routine.
     */
    @HillviewRpc
    public void heavyHittersSampling(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        SampleHeavyHittersSketch shh = new SampleHeavyHittersSketch(info.columns,
                info.amount/100, info.totalRows, info.seed);
        PostProcessedSketch<ITable, FreqKListSample, TopList> post =
                shh.andThen(result -> {
                    HillviewComputation computation = context.getComputation(request);
                    // This allocates a new RpcTarget object and registers it.
                    HeavyHittersTarget target = new HeavyHittersTarget(result, computation);
                    NextKList top = result.getTop(info.columns);
                    return new TopList(top, target.getId().toString());
                });
        this.runCompleteSketch(this.table, post, request, context);
    }

    static class HeavyHittersFilterInfo {
        String hittersId = "";
        @SuppressWarnings("NotNullFieldNotInitialized")
        Schema schema;
        boolean includeSet;
    }

    /**
     * Runs the ExactFreqSketch method on a candidate list of heavy hitters.
     */
    @HillviewRpc
    public void checkHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        RpcObjectManager.instance.when(hhi.hittersId, rpcTarget -> {
            HeavyHittersTarget hht = rpcTarget.to(HeavyHittersTarget.class);
            ExactFreqSketch efSketch = new ExactFreqSketch(hhi.schema, hht.heavyHitters);
            PostProcessedSketch<ITable, FreqKListExact, TopList> post = efSketch.andThen(result -> {
                HillviewComputation computation = context.getComputation(request);
                // This allocates a new RpcTarget object and registers it.
                HeavyHittersTarget target = new HeavyHittersTarget(result, computation);
                NextKList top = result.getTop(hhi.schema);
                return new TopList(top, target.getId().toString());
            });
            TableTarget.this.runCompleteSketch(TableTarget.this.table, post, request, context);
        });
    }

    static class HeavyHittersListFilterInfo extends HeavyHittersFilterInfo {
        int[] rowIndices = new int[0];
    }

    /**
     * Creates a table containing/excluding the selected heavy hitters.
     */
    @HillviewRpc
    public void filterListHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersListFilterInfo hhl = request.parseArgs(HeavyHittersListFilterInfo.class);
        RpcObjectManager.instance.when(hhl.hittersId, rpcTarget -> {
            HeavyHittersTarget hht = rpcTarget.to(HeavyHittersTarget.class);
            ITableFilterDescription filter = hht.heavyHitters.getFilter(hhl.schema,
                    hhl.includeSet, hhl.rowIndices);
            TableTarget.this.runFilter(filter, request, context);
        });
    }

    /**
     * Creates a table containing/excluding all the heavy hitters.
     */
    @HillviewRpc
    public void filterHeavy(RpcRequest request, RpcRequestContext context) {
        HeavyHittersFilterInfo hhi = request.parseArgs(HeavyHittersFilterInfo.class);
        RpcObjectManager.instance.when(hhi.hittersId, rpcTarget -> {
            HeavyHittersTarget hht = rpcTarget.to(HeavyHittersTarget.class);
            ITableFilterDescription filter = hht.heavyHitters.getFilter(hhi.schema, hhi.includeSet);
            TableTarget.this.runFilter(filter, request, context);
        });
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        // TODO: should compute confidence of result somehow instead of using 0
        NoisyHLogLog nhll = new NoisyHLogLog(sketch, new Noise());
        this.runSketch(this.table, nhll, request, context);
    }

    @HillviewRpc
    public void convertColumn(RpcRequest request, RpcRequestContext context) {
        ConvertColumnMap.Info info = request.parseArgs(ConvertColumnMap.Info.class);
        ConvertColumnMap map = new ConvertColumnMap(info);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void createIntervalColumn(RpcRequest request, RpcRequestContext context) {
        CreateIntervalColumnMap.Info info = request.parseArgs(CreateIntervalColumnMap.Info.class);
        CreateIntervalColumnMap map = new CreateIntervalColumnMap(info);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @Override
    public String toString() {
        return "TableTarget object=" + super.toString();
    }

    static class SetOperationInfo {
        String op = "";
        String otherId = "";
    }

    @HillviewRpc
    public void setOperation(RpcRequest request, RpcRequestContext context) {
        SetOperationInfo op = request.parseArgs(SetOperationInfo.class);
        SetOperationMap sm = new SetOperationMap(op.op);
        RpcObjectManager.instance.when(op.otherId, target -> {
            TableTarget otherTable = (TableTarget)target;
            TableTarget.this.runZip(
                    TableTarget.this.table, otherTable.table, sm, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
        });
    }

    @SuppressWarnings("NotNullFieldNotInitialized")
    static class CompareDatasetsInfo {
        String[] names;
        String[] otherIds;
        String outputName = "";
    }

    @HillviewRpc
    public void compareDatasets(RpcRequest request, RpcRequestContext context) {
        CompareDatasetsInfo op = request.parseArgs(CompareDatasetsInfo.class);
        SetCompareColumnMap map = new SetCompareColumnMap(op.outputName, Utilities.list(op.names));
        List<String> ids = Utilities.list(op.otherIds);
        if (op.names.length != op.otherIds.length)
            throw new RuntimeException("Names and ids must have the same length");
        RpcObjectManager.instance.when(
                ids, l -> {
                    List<IDataSet<ITable>> other = Linq.map(l, e -> e.to(TableTarget.class).table);
                    TableTarget.this.runZipN(
                            TableTarget.this.table, other, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
                });
    }

    @HillviewRpc
    public void jsCreateColumn(RpcRequest request, RpcRequestContext context) {
        CreateColumnJSMap.Info info = request.parseArgs(CreateColumnJSMap.Info.class);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void jsFilter(RpcRequest request, RpcRequestContext context) {
        JSFilterDescription.Info filter = request.parseArgs(JSFilterDescription.Info.class);
        JSFilterDescription desc = new JSFilterDescription(filter);
        FilterMap map = new FilterMap(desc);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void filterList(RpcRequest request, RpcRequestContext context) {
        FilterListDescription filter = request.parseArgs(FilterListDescription.class);
        FilterMap map = new FilterMap(filter);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void kvCreateColumn(RpcRequest request, RpcRequestContext context) {
        ExtractValueFromKeyMap.Info info = request.parseArgs(ExtractValueFromKeyMap.Info.class);
        ExtractValueFromKeyMap map = new ExtractValueFromKeyMap(info);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void project(RpcRequest request, RpcRequestContext context) {
        Schema proj = request.parseArgs(Schema.class);
        ProjectMap map = new ProjectMap(proj);
        this.runMap(this.table, map, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }
}
