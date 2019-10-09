package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.IdMap;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.utils.Converters;
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
        DPWrapper.PrivateHistogramArgs[] info = request.parseArgs(DPWrapper.PrivateHistogramArgs[].class);
        ColumnPrivacyMetadata metadata = this.wrapper.privacySchema.get(info[0].cd.name);
        double epsilon = metadata.epsilon;

        HistogramSketch sk = info[0].getSketch(metadata);
        HistogramSketch cdf = info[1].getSketch(metadata);
        IDyadicDecomposition dd = info[0].getDecomposition(metadata);
        IDyadicDecomposition cdd = info[1].getDecomposition(metadata);
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(sk, cdf);
        this.runCompleteSketch(this.table, csk, (e, c) ->
                new Pair<PrivateHistogram, PrivateHistogram>(
                        new PrivateHistogram(dd, Converters.checkNull(e.first), epsilon, false),
                        new PrivateHistogram(cdd, Converters.checkNull(e.second), epsilon, true)), request, context);
    }

    @HillviewRpc
    public void getDataRanges1D(RpcRequest request, RpcRequestContext context) {
        RangeArgs[] args = request.parseArgs(RangeArgs[].class);
        assert args.length == 1;
        double min, max;
        DoubleColumnPrivacyMetadata md = (DoubleColumnPrivacyMetadata)this.wrapper.privacySchema.get(
                args[0].cd.name);
        RangeFilterDescription filter = this.wrapper.columnLimits.get(args[0].cd.name);
        if (filter == null) {
            min = md.globalMin;
            max = md.globalMax;
        } else {
            min = md.roundDown(filter.min);
            max = md.roundUp(filter.max);
        }

        DataRange retRange = new DataRange(min, max);
        retRange.presentCount = -1;
        retRange.missingCount = -1;
        PrecomputedSketch<ITable, DataRange> sk =
                new PrecomputedSketch<ITable, DataRange>(retRange, new DoubleDataRangeSketch(args[0].cd.name));
        BiFunction<DataRange, HillviewComputation, JsonList<BucketsInfo>> post = (e, c) -> {
            JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
            result.add(e);
            return result;
        };
        this.runCompleteSketch(this.table, sk, post, request, context);
    }

    @HillviewRpc
    public void filterRange(RpcRequest request, RpcRequestContext context) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        IdMap<ITable> map = new IdMap<ITable>();
        this.runMap(this.table, map, (e, c) -> {
            PrivateTableTarget result = new PrivateTableTarget(PrivateTableTarget.this, c);
            result.wrapper.columnLimits.put(filter.cd.name, filter);
            return result;
        }, request, context);
    }

    @HillviewRpc
    public void hLogLog(RpcRequest request, RpcRequestContext context) {
        DistinctCountRequestInfo col = request.parseArgs(DistinctCountRequestInfo.class);
        HLogLogSketch sketch = new HLogLogSketch(col.columnName, col.seed);
        // TODO: add noise to this count
        this.runSketch(this.table, sketch, request, context);
    }

    @HillviewRpc
    public void heavyHitters(RpcRequest request, RpcRequestContext context) {
        HeavyHittersRequestInfo info = request.parseArgs(HeavyHittersRequestInfo.class);
        // TODO: process data to round to bucket boundaries
        MGFreqKSketch sk = new MGFreqKSketch(info.columns, info.amount/100);
        // TODO: add noise to the counts
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

    // For numeric-valued private histograms, this function returns the global min/max
    // specified by the curator if no range is provided, and otherwise just returns
    // the user-provided range.
    @HillviewRpc
    public void getDataRanges2D(RpcRequest request, RpcRequestContext context) {
        RangeArgs[] args = request.parseArgs(RangeArgs[].class);
        assert args.length == 2;

        DataRange[] precomputed = new DataRange[2];
        for (int i = 0; i < 2; i++) {
            double min, max;
            DoubleColumnPrivacyMetadata md = (DoubleColumnPrivacyMetadata)this.wrapper.privacySchema.get(
                    args[i].cd.name);
            RangeFilterDescription filter = this.wrapper.columnLimits.get(args[0].cd.name);
            if (filter == null) {
                min = md.globalMin;
                max = md.globalMax;
            } else {
                min = md.roundDown(filter.min);
                max = md.roundUp(filter.max);
            }

            DataRange retRange = new DataRange(min, max);
            retRange.presentCount = -1;
            retRange.missingCount = -1;
            precomputed[i] = retRange;
        }
        PrecomputedSketch<ITable, Pair<DataRange, DataRange>> sk =
                new PrecomputedSketch<ITable, Pair<DataRange, DataRange>>(
                        new Pair<DataRange, DataRange>(precomputed[0], precomputed[1]),
                        new ConcurrentSketch<ITable, DataRange, DataRange>(
                            new DoubleDataRangeSketch(args[0].cd.name),
                            new DoubleDataRangeSketch(args[1].cd.name)));
        BiFunction<Pair<DataRange, DataRange>, HillviewComputation, JsonList<BucketsInfo>> post = (e, c) -> {
            JsonList<BucketsInfo> result = new JsonList<BucketsInfo>(1);
            result.add(e.first);
            result.add(e.second);
            return result;
        };
        this.runCompleteSketch(this.table, sk, post, request, context);
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        DPWrapper.PrivateHistogramArgs[] info = request.parseArgs(DPWrapper.PrivateHistogramArgs[].class);
        assert info.length == 2;
        ColumnPrivacyMetadata col0 = this.wrapper.privacySchema.get(info[0].cd.name);
        ColumnPrivacyMetadata col1 = this.wrapper.privacySchema.get(info[1].cd.name);
        // Epsilon value has to be specified on the pair, separately from each column.
        double epsilon =this.wrapper.privacySchema.get(new String[] {
                info[0].cd.name, info[1].cd.name}).epsilon;

        IDyadicDecomposition d0 = info[0].getDecomposition(col0);
        IDyadicDecomposition d1 = info[1].getDecomposition(col1);
        IHistogramBuckets b0 = d0.getHistogramBuckets();
        IHistogramBuckets b1 = d1.getHistogramBuckets();
        HeatmapSketch sk = new HeatmapSketch(b0, b1, info[0].cd.name, info[1].cd.name, 1.0, 0);
        this.runCompleteSketch(this.table, sk, (e, c) ->
                new PrivateHeatmap(d0, d1, e, epsilon).heatmap, request, context);
    }
}
