package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.IdMap;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.filters.RangeFilterPair;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.JsonList;

import java.util.function.BiFunction;

public class PrivateTableTarget extends RpcTarget {
    private final IDataSet<ITable> table;
    private final DPWrapper wrapper;

    PrivateTableTarget(IDataSet<ITable> table, HillviewComputation computation,
                       PrivacySchema privacySchema) {
        super(computation);
        this.wrapper = new DPWrapper(privacySchema);
        this.table = table;
        this.registerObject();
    }

    private PrivateTableTarget(PrivateTableTarget other, HillviewComputation computation) {
        super(computation);
        this.table = other.table;
        this.wrapper = new DPWrapper(other.wrapper);
        this.registerObject();
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        SummarySketch ss = new SummarySketch();
        this.runCompleteSketch(this.table, ss, (d, c) -> this.wrapper.addPrivateMetadata(d), request, context);
    }

    // Returns both the histogram and the precomputed CDF of the data.
    // Each histogram data structure will also contain the corresponding precomputed CDF,
    // but we still compute two of them for one request because the histogram buckets and CDF
    // are computed at different bucket granularities.
    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnQuantization quantization = this.wrapper.privacySchema.quantization(info[0].cd.name);
        Converters.checkNull(quantization);
        HistogramSketch sk = info[0].getSketch(quantization); // Histogram
        HistogramSketch cdf = info[1].getSketch(quantization);
        IDyadicDecomposition d0 = info[0].getDecomposition(quantization);
        IDyadicDecomposition d1 = info[1].getDecomposition(quantization);
        double epsilon = this.wrapper.privacySchema.epsilon(info[0].cd.name);
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(sk, cdf);
        this.runCompleteSketch(this.table, csk, (e, c) ->
                new Pair<PrivateHistogram, PrivateHistogram>(
                        new PrivateHistogram(d0, Converters.checkNull(e.first), epsilon, false),
                        new PrivateHistogram(d1, Converters.checkNull(e.second), epsilon, true)),
                request, context);
    }

    @HillviewRpc
    public void getDataQuantiles1D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 1;
        BucketsInfo retRange = this.wrapper.getRange(args[0]);
        PrecomputedSketch<ITable, BucketsInfo> sk = new PrecomputedSketch<ITable, BucketsInfo>(retRange);
        this.runCompleteSketch(this.table, sk, (e, c) -> new JsonList<BucketsInfo>(e), request, context);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(PrivateTableTarget.this, c);
            result.wrapper.columnLimits.put(filter.cd.name, filter);
            return result;
        }, request, context);
    }

    @HillviewRpc
    public void filter2DRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterPair filter = request.parseArgs(RangeFilterPair.class);
        if (filter.first.complement || filter.second.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(PrivateTableTarget.this, c);
            result.wrapper.columnLimits.put(filter.first.cd.name, filter.first);
            result.wrapper.columnLimits.put(filter.second.cd.name, filter.second);
            return result;
        }, request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        // TODO(pratiksha): add noise to this count
        this.runSketch(this.table, sketch, request, context);
    }

    @HillviewRpc
    public void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        MGFreqKSketch sk = new MGFreqKSketch(info.columns, info.amount/100,
                this.wrapper.privacySchema.quantization);
        // TODO(pratiksha): add noise to the counts
        this.runCompleteSketch(this.table, sk, (x, c) -> TableTarget.getTopList(x, info.columns, c),
                request, context);
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
    public void getDataQuantiles2D(RpcRequest request, RpcRequestContext context) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 2;
        BucketsInfo retRange0 = this.wrapper.getRange(args[0]);
        BucketsInfo retRange1 = this.wrapper.getRange(args[1]);
        PrecomputedSketch<ITable, Pair<BucketsInfo, BucketsInfo>> sk =
                new PrecomputedSketch<ITable, Pair<BucketsInfo, BucketsInfo>>(
                        new Pair<BucketsInfo, BucketsInfo>(retRange0, retRange1));
        BiFunction<Pair<BucketsInfo, BucketsInfo>, HillviewComputation, JsonList<BucketsInfo>> post =
                (e, c) -> new JsonList<BucketsInfo>(e.first, e.second);
        this.runCompleteSketch(this.table, sk, post, request, context);
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        HistogramRequestInfo[] info = request.parseArgs(HistogramRequestInfo[].class);
        assert info.length == 2;
        ColumnQuantization q0 = this.wrapper.privacySchema.quantization(info[0].cd.name);
        ColumnQuantization q1 = this.wrapper.privacySchema.quantization(info[1].cd.name);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        double epsilon = this.wrapper.privacySchema.epsilon(new String[] {
                info[0].cd.name, info[1].cd.name});
        IHistogramBuckets b0 = info[0].getBuckets(q0);
        IHistogramBuckets b1 = info[1].getBuckets(q1);
        IDyadicDecomposition d0 = info[0].getDecomposition(q0);
        IDyadicDecomposition d1 = info[1].getDecomposition(q1);
        HeatmapSketch sk = new HeatmapSketch(
                b0, b1, info[0].cd.name, info[1].cd.name, 1.0, 0, q0, q1);
        this.runCompleteSketch(this.table, sk, (e, c) ->
                new PrivateHeatmap(d0, d1, e, epsilon).heatmap, request, context);
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        TableTarget.NextKArgs nextKArgs = request.parseArgs(TableTarget.NextKArgs.class);
        RowSnapshot rs = TableTarget.asRowSnapshot(
                nextKArgs.firstRow, nextKArgs.order, nextKArgs.columnsNoValue);
        NextKSketch nk = new NextKSketch(nextKArgs.order, null, rs, nextKArgs.rowsOnScreen,
                this.wrapper.privacySchema.quantization);
        // TODO(pratiksha): add noise to the counts on the NextKList
        this.runSketch(this.table, nk, request, context);
    }

    @HillviewRpc
    public void quantile(RpcRequest request, RpcRequestContext context) {
        QuantileInfo info = request.parseArgs(QuantileInfo.class);
        SampleQuantileSketch sk = new SampleQuantileSketch(
                info.order, info.precision, info.tableSize, info.seed,
                this.wrapper.privacySchema.quantization);
        BiFunction<SampleList, HillviewComputation, RowSnapshot> getRow = (ql, c) -> ql.getRow(info.position);
        this.runCompleteSketch(this.table, sk, getRow, request, context);
    }
}
