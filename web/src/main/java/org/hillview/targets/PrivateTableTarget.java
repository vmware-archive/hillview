package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.sketches.Histogram;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.SummarySketch;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.PrivacyMetadata;

import java.util.HashMap;
import java.util.function.BiFunction;

/* This class is like TableTarget, but stores additional metadata to implement the binary mechanism
 * for histograms and heat maps. */
public class PrivateTableTarget extends RpcTarget {
    private final IDataSet<ITable> table;

    /* Global parameters for differentially-private histograms using the binary mechanism. */
    private PrivacySchema metadata;

    public long seed; // Global seed for PRNG

    PrivateTableTarget(IDataSet<ITable> table, HillviewComputation computation,
                       PrivacySchema metadata) {
        super(computation);
        this.table = table;
        this.registerObject();

        this.metadata = metadata;
    }

    public class PrivacySummary implements IJson {
        public Schema schema;
        public int rowCount = 0;
        public PrivacySchema metadata;
    }

    private PrivacySummary postprocessSummary(SummarySketch.TableSummary summary) {
        PrivacySummary pSumm = new PrivacySummary();
        pSumm.schema = summary.schema;
        pSumm.metadata = this.metadata;
        return pSumm;
    }

    @HillviewRpc
    public void getSchema(RpcRequest request, RpcRequestContext context) {
        SummarySketch ss = new SummarySketch();
        this.runCompleteSketch(this.table, ss, (d, c) -> postprocessSummary(d), request, context);
    }

    class PrivateHistogramArgs {
        ColumnDescription cd = new ColumnDescription();
        double samplingRate = 1.0; // Fix to exact count
        long seed;

        double min;
        double max;
        int bucketCount;

        DyadicHistogramBuckets getBuckets() {
            assert (!cd.kind.isString());

            // This bucket class ensures that computed buckets fall on leaf boundaries.
            return new DyadicHistogramBuckets(this.min, this.max,
                    this.bucketCount, PrivateTableTarget.this.metadata.get(cd.name).granularity);
        }

        HistogramSketch getSketch() {
            DyadicHistogramBuckets buckets = this.getBuckets();
            return new HistogramSketch(buckets, this.cd.name, this.samplingRate, this.seed);
        }

        // add noise to result
        BiFunction<Histogram, HillviewComputation, Histogram> getPostProcessing() {
            double leaves = (max-min) / PrivateTableTarget.this.metadata.get(cd.name).granularity;
            double scale = (Math.log(leaves) / (PrivateTableTarget.this.metadata.get(cd.name).epsilon * Math.log(2)));

            return (e, c) -> {
                e.addDyadicLaplaceNoise(scale);
                return e;
            };
        }
    }

    @HillviewRpc
    public void privateHistogram(RpcRequest request, RpcRequestContext context) {
        PrivateHistogramArgs info = request.parseArgs(PrivateHistogramArgs.class);
        HistogramSketch sk = info.getSketch();
        this.runCompleteSketch(this.table, sk, info.getPostProcessing(), request, context);
    }
}
