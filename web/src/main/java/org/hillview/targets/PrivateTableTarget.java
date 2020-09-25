package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.api.TableSketch;
import org.hillview.sketches.highorder.*;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.FilterMap;
import org.hillview.maps.ProjectMap;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.PrivacySchema;
import org.hillview.table.QuantizationSchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.filters.RangeFilterArrayDescription;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.util.function.BiFunction;

public class PrivateTableTarget extends TableRpcTarget implements IPrivateDataset {
    static final long serialVersionUID = 1;
    public final DPWrapper wrapper;

    PrivateTableTarget(IDataSet<ITable> table, HillviewComputation computation,
                       PrivacySchema privacySchema, @Nullable String schemaFilename) {
        super(computation, schemaFilename);
        this.wrapper = new DPWrapper(privacySchema, schemaFilename);
        this.setTable(table);
        this.registerObject();
    }

    private PrivateTableTarget(IDataSet<ITable> table,
                               HillviewComputation computation,
                               DPWrapper wrapper, @Nullable String metadataDirectory) {
        super(computation, metadataDirectory);
        this.table = table;
        this.wrapper = new DPWrapper(wrapper);
        this.registerObject();
    }

    private PrivacySchema getPrivacySchema() {
        return this.wrapper.getPrivacySchema();
    }

    @HillviewRpc
    public void changePrivacy(RpcRequest request, RpcRequestContext context) {
        this.wrapper.setPrivacySchema(request.parseArgs(PrivacySchema.class));
        HillviewLogger.instance.info("Updated privacy schema");
        this.returnResult(new JsonInString("{}"), request, context);
    }

    @HillviewRpc
    public void savePrivacy(RpcRequest request, RpcRequestContext context) {
        this.wrapper.savePrivacySchema();
        HillviewLogger.instance.info("Saved privacy schema");
        this.returnResult(new JsonInString("{}"), request, context);
    }

    @HillviewRpc
    public void getMetadata(RpcRequest request, RpcRequestContext context) {
        GeoFileInformation[] info = this.getGeoFileInformation();
        SummarySketch ss = new SummarySketch();
        PostProcessedSketch<ITable, TableSummary, DPWrapper.TableMetadata> post = ss.andThen(
                s -> new TableMetadata(s, info)).andThen(
                        PrivateTableTarget.this.wrapper::addPrivateMetadata);
        this.runCompleteSketch(this.table, post, request, context);
    }

    // Returns both the histogram and the precomputed CDF of the data.
    // Each histogram data structure will also contain the corresponding precomputed CDF,
    // but we still compute two of them for one request because the histogram buckets and CDF
    // are computed at different bucket granularities.
    @HillviewRpc
    public void histogramAndCDF(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        ColumnQuantization quantization = this.getPrivacySchema().quantization(
                info.histos[0].cd.name);
        Converters.checkNull(quantization);
        TableSketch<Groups<Count>> sk = info.getSketch(0, quantization); // Histogram
        TableSketch<Groups<Count>> cdf = info.getSketch(1, quantization);
        IntervalDecomposition d0 = info.getDecomposition(0, quantization);
        IntervalDecomposition d1 = info.getDecomposition(1, quantization);
        double epsilon = this.getPrivacySchema().epsilon(info.histos[0].cd.name);
        DPHistogram<Groups<Count>> privateHisto = new DPHistogram<>(
                sk, this.wrapper.getColumnIndex(info.histos[0].cd.name),
                d0, epsilon, false, this.wrapper.laplace);
        DPHistogram<Groups<Count>> privateCdf = new DPHistogram<>(
                cdf, this.wrapper.getColumnIndex(info.histos[0].cd.name),
                d1, epsilon, true, this.wrapper.laplace);
        ConcurrentPostprocessedSketch<ITable, Groups<Count>, Groups<Count>,
                Two<JsonGroups<Count>>, Two<JsonGroups<Count>>> ccp =
                new ConcurrentPostprocessedSketch<>(privateHisto, privateCdf);
        this.runCompleteSketch(this.table, ccp, request, context);
    }

    @SuppressWarnings("unused")
    @HillviewRpc
    public void getDataQuantiles(RpcRequest request, RpcRequestContext context) {
        this.wrapper.getDataQuantiles(request, context, this);
    }

    @HillviewRpc
    public void filterRanges(RpcRequest request, RpcRequestContext context) {
        RangeFilterArrayDescription filter = request.parseArgs(RangeFilterArrayDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        FilterMap map = new FilterMap(filter, this.getPrivacySchema().quantization);
        BiFunction<IDataSet<ITable>, HillviewComputation, IRpcTarget> constructor = (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(e, c, this.wrapper, this.metadataDirectory);
            for (RangeFilterDescription f: filter.filters)
                result.getWrapper().filter(f);
            return result;
        };
        this.runMap(this.table, map, constructor, request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        double epsilon = this.wrapper.getPrivacySchema().epsilon(col.columnName);
        Noise noise = DPWrapper.computeCountNoise(this.wrapper.getColumnIndex(col.columnName),
                DPWrapper.SpecialBucket.DistinctCount, epsilon, this.wrapper.laplace);
        NoisyHLogLog nhll = new NoisyHLogLog(sketch, noise);
        this.runCompleteSketch(this.table, nhll, request, context);
    }

    @HillviewRpc
    public void histogram2D(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo info = request.parseArgs(HistogramRequestInfo.class);
        assert info.size() == 2;
        Histogram2DSketch sk = new Histogram2DSketch(
                info.getBuckets(1),
                info.getBuckets(0));
        ColumnQuantization q0 = this.getPrivacySchema().quantization(info.histos[0].cd.name);
        ColumnQuantization q1 = this.getPrivacySchema().quantization(info.histos[1].cd.name);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        IntervalDecomposition d0 = info.getDecomposition(0, q0);
        IntervalDecomposition d1 = info.getDecomposition(1, q1);
        double epsilon = this.getPrivacySchema().epsilon(
                info.histos[0].cd.name, info.histos[1].cd.name);
        DPHeatmapSketch<Groups<Count>, Groups<Groups<Count>>> hsk = new DPHeatmapSketch<>(
                sk.quantized(new QuantizationSchema(q0, q1)),
                this.wrapper.getColumnIndex(info.histos[0].cd.name, info.histos[1].cd.name),
                d0, d1, epsilon, this.wrapper.laplace);
        this.runSketch(this.table, hsk, request, context);
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        TableTarget.NextKArgs nextKArgs = request.parseArgs(TableTarget.NextKArgs.class);
        // Only allow this if the sort order is empty
        if (nextKArgs.order.getSize() != 0)
            throw new HillviewException("No column data can be displayed privately");
        RowSnapshot rs = TableTarget.asRowSnapshot(
                nextKArgs.firstRow, nextKArgs.order, nextKArgs.columnsNoValue);
        NextKSketch nk = new NextKSketch(nextKArgs.order, null, rs, nextKArgs.rowsOnScreen,
                this.getPrivacySchema().quantization);
        double epsilon = this.wrapper.getPrivacySchema().epsilon();
        Noise noise = DPWrapper.computeCountNoise(DPWrapper.TABLE_COLUMN_INDEX, // Computed on entire table
                DPWrapper.SpecialBucket.TotalCount, epsilon, this.wrapper.laplace);
        NextKSketchNoisy skn = new NextKSketchNoisy(nk, noise);
        this.runCompleteSketch(this.table, skn, request, context);
    }

    @HillviewRpc
    public void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(
                info.order, info.precision, info.tableSize, info.seed,
                this.getPrivacySchema().quantization);
        PostProcessedSketch<ITable, SampleList, RowSnapshot> post =
                sk.andThen(s -> s.getRow(info.position));
        this.runCompleteSketch(this.table, post, request, context);
    }

    @HillviewRpc
    public void project(RpcRequest request, RpcRequestContext context) {
        Schema proj = request.parseArgs(Schema.class);
        ProjectMap map = new ProjectMap(proj);
        this.runMap(this.table, map, (d, c) ->
                new PrivateTableTarget(d, c, this.wrapper, this.metadataDirectory), request, context);
    }

    @Override
    public IDataSet<ITable> getDataset() {
        return this.table;
    }

    @Override
    public DPWrapper getWrapper() {
        return this.wrapper;
    }
}
