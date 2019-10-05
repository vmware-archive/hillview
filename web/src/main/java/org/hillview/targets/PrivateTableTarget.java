package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.table.rows.PrivacyMetadata;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * This class represents a remote dataset that can only be accessed using differentially-private operations.
 */
public class PrivateTableTarget extends RpcTarget {
    private final IDataSet<ITable> table;
    // For each column the range allowed after filtering
    private final HashMap<String, RangeFilterDescription> columnLimits;
    public long seed; // Global seed for PRNG
    /* Global parameters for differentially-private histograms using the binary mechanism. */
    private PrivacySchema privacySchema;

    PrivateTableTarget(IDataSet<ITable> table, HillviewComputation computation,
                       PrivacySchema privacySchema) {
        super(computation);
        this.table = table;
        this.columnLimits = new HashMap<String, RangeFilterDescription>();
        this.registerObject();
        this.privacySchema = privacySchema;
    }

    private PrivateTableTarget(PrivateTableTarget other, HillviewComputation computation) {
        super(computation);
        this.table = other.table;
        this.privacySchema = other.privacySchema;
        this.registerObject();
        this.columnLimits = new HashMap<String, RangeFilterDescription>(other.columnLimits);
    }

    public static class PrivacySummary implements IJson {
        @Nullable
        public Schema schema;
        public long rowCount;
        @Nullable
        public PrivacySchema metadata;
    }

    private PrivacySummary addPrivateMetadata(TableSummary summary) {
        PrivacySummary pSumm = new PrivacySummary();
        pSumm.schema = summary.schema;
        pSumm.metadata = this.privacySchema;
        // TODO: add noise to the row count too.
        pSumm.rowCount = summary.rowCount;
        return pSumm;
    }

    @HillviewRpc
    public void getSummary(RpcRequest request, RpcRequestContext context) {
        SummarySketch ss = new SummarySketch();
        this.runCompleteSketch(this.table, ss, (d, c) -> addPrivateMetadata(d), request, context);
    }

    static class PrivateHistogramArgs {
        ColumnDescription cd = new ColumnDescription();
        double samplingRate = 1.0; // Fix to exact count
        long seed;

        double min;
        double max;
        int bucketCount;

        DyadicDoubleHistogramBuckets getBuckets(PrivacySchema metadata) {
            if (!cd.kind.isNumeric())
                throw new RuntimeException("Attempted to instantiate private buckets with non-numeric column");

            // This bucket class ensures that computed buckets fall on leaf boundaries.
        HistogramSketch getSketch(PrivacySchema metadata) {
            DyadicDoubleHistogramBuckets buckets = this.getBuckets(metadata);
            return new HistogramSketch(buckets, this.cd.name, this.samplingRate, this.seed, null);
        }

        ColumnPrivacyMetadata getMetadata(PrivacySchema metadata) {
            return metadata.get(cd.name);
        }
    }

    // Returns both the histogram and the precomputed CDF of the data.
    // Each histogram data structure will also contain the corresponding precomputed CDF,
    // but we still compute two of them for one request because the histogram buckets and CDF
    // are computed at different bucket granularities.
    @HillviewRpc
    public void histogram(RpcRequest request, RpcRequestContext context) {
        PrivateHistogramArgs[] info = request.parseArgs(PrivateHistogramArgs[].class);
        info[0].initMetadata(privacySchema);
        info[1].initMetadata(privacySchema);
        double epsilon = info[0].metadata.epsilon;

        HistogramSketch sk = info[0].getSketch(); // Histogram
        HistogramSketch cdf = info[1].getSketch(); // CDF
        ConcurrentSketch<ITable, Histogram, Histogram> csk =
                new ConcurrentSketch<ITable, Histogram, Histogram>(sk, cdf);
        this.runCompleteSketch(this.table, csk,
                (e, c) -> new Pair<PrivateHistogram, PrivateHistogram>(
                        new PrivateHistogram(e.first, epsilon, false),
                        new PrivateHistogram(e.second, epsilon, true)), request, context);
    }

    @HillviewRpc
    public void getDataRanges1D(RpcRequest request, RpcRequestContext context) {
        RangeArgs[] args = request.parseArgs(RangeArgs[].class);
        assert args.length == 1;
        double min, max;
        DoubleColumnPrivacyMetadata md = (DoubleColumnPrivacyMetadata)this.metadata.get(args[0].cd.name);
        RangeFilterDescription filter = this.columnLimits.get(args[0].cd.name);
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
        this.createTargetDirect(request, context, (c) -> {
            PrivateTableTarget result = new PrivateTableTarget(this, c);
            result.columnLimits.put(filter.cd.name, filter);
            return result;
        });
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
        FreqKSketchMG sk = new FreqKSketchMG(info.columns, info.amount/100);
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
        PrivateTableTarget.PrivateRangeArgs[] args = request.parseArgs(PrivateTableTarget.PrivateRangeArgs[].class);
        assert args.length == 1;

        JsonList<DataRange> rangeList = new JsonList<>();
        for (int i = 0; i < 2; i++) {
            double min, max;
            PrivacyMetadata md = this.privacySchema.get(args[i].cd.name);
            if (args[i].min == null) {
                min = md.globalMin;
            } else {
                min = args[i].min;
            }

            if (args[i].max == null) {
                max = md.globalMax;
            } else {
                max = args[i].max;
            }

            DataRange retRange = new DataRange(min, max);
            retRange.presentCount = -1;
            retRange.missingCount = -1;

            rangeList.add(retRange);
        }
        constructAndSendReply(rangeList, request, context);
    }

    // compute CDF on the second histogram (at finer CDF granularity)
    static BiFunction<Heatmap,
                HillviewComputation,
                Heatmap> makePrivateHeatmapFunction(double epsilon) {
        return (e, c) -> new PrivateHeatmap(e, epsilon).heatmap;
    }

    @HillviewRpc
    public void heatmap(RpcRequest request, RpcRequestContext context) {
        PrivateTableTarget.PrivateHistogramArgs[] info = request.parseArgs(PrivateTableTarget.PrivateHistogramArgs[].class);
        assert info.length == 2;

        info[0].initMetadata(privacySchema);
        info[1].initMetadata(privacySchema);

        // Epsilon value has to be specified on the pair, separately from each column.
        double epsilon = privacySchema.get(new String[] {info[0].cd.name, info[1].cd.name}).epsilon;

        HeatmapSketch sk = new HeatmapSketch(
                info[0].getBuckets(),
                info[1].getBuckets(),
                info[0].cd.name,
                info[1].cd.name, 1.0, 0);
        this.runCompleteSketch(this.table, sk, makePrivateHeatmapFunction(epsilon), request, context);
    }
}
