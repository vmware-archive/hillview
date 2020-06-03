package org.hillview.sketches;

import org.hillview.dataset.IncrementalTableSketch;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;
import org.hillview.utils.SerializableBiFunction;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Given a TableSketch S, this applies S to each group.
 * The groups are defined by buckets, given by IHistogramBuckets.
 * @param <R>  Result computed by the basic sketch S.
 * @param <S>  The type of a a table sketch.
 * @param <G>  The type of the result produced.
 */
public class GroupBySketch<
        R extends ISketchResult,
        SW extends ISketchWorkspace,
        S extends IncrementalTableSketch<R, SW>,
        G extends Groups<R>>
        extends IncrementalTableSketch<G, GroupByWorkspace<SW>> {
    private final SerializableBiFunction<JsonList<R>, R, G> groupFactory;
    protected final S missingSketch;
    protected final JsonList<S> bucketSketch;
    protected final IHistogramBuckets buckets;

    /**
     * Create a sketch that computes groups of sketches.
     * @param buckets         Buckets defining groups.
     * @param groupFactory    A function that creates a group from an array of bucket results and a missing bucket result.
     * @param sketchFactory   A function that creates the sketches for each bucket given an index.
     *                        The function is called with index -1 to create the missing bucket sketch.
     */
    protected GroupBySketch(IHistogramBuckets buckets,
                            SerializableBiFunction<JsonList<R>, R, G> groupFactory,
                            Function<Integer, S> sketchFactory) {
        this.missingSketch = sketchFactory.apply(-1);
        this.bucketSketch = new JsonList<S>(buckets.getBucketCount());
        for (int i = 0; i < buckets.getBucketCount(); i++)
            this.bucketSketch.add(sketchFactory.apply(i));
        this.buckets = buckets;
        this.groupFactory = groupFactory;
    }

    protected GroupBySketch(IHistogramBuckets buckets,
                            SerializableBiFunction<JsonList<R>, R, G> groupFactory,
                            S sketch) {
        this.missingSketch = sketch;
        this.bucketSketch = new JsonList<S>(buckets.getBucketCount());
        for (int i = 0; i < buckets.getBucketCount(); i++)
            this.bucketSketch.add(sketch);
        this.buckets = buckets;
        this.groupFactory = groupFactory;
    }

    @Override
    public void add(GroupByWorkspace<SW> workspace, G result, int rowNumber) {
        if (workspace.column.isMissing(rowNumber)) {
            this.missingSketch.add(workspace.missingWorkspace, result.perMissing, rowNumber);
        } else {
            int index = this.buckets.indexOf(workspace.column, rowNumber);
            if (index >= 0 && index < result.perBucket.size())
                this.bucketSketch.get(index).add(workspace.bucketWorkspace.get(index), result.perBucket.get(index), rowNumber);
            /*
            else
                this.sketch.add(workspace.childWorkspace, result.outOfRange, rowNumber);
             */
        }
    }

    @Override
    public GroupByWorkspace<SW> initialize(ITable data) {
        IColumn column = Converters.checkNull(data).getLoadedColumn(this.buckets.getColumn());
        SW missing = this.missingSketch.initialize(data);
        JsonList<SW> bucketWorkspaces = Linq.map(this.bucketSketch, s -> s.initialize(data));
        return new GroupByWorkspace<SW>(column, bucketWorkspaces, missing);
    }

    @Override
    public G zero() {
        int b = this.buckets.getBucketCount();
        JsonList<R> perBucket = new JsonList<R>(b);
        for (int i = 0; i < b; i++)
            perBucket.add(this.bucketSketch.get(i).zero());
        R perMissing = this.missingSketch.zero();
        //R noBucket = this.sketch.zero();
        return this.groupFactory.apply(perBucket, Converters.checkNull(perMissing)
                /*, Converters.checkNull(noBucket) */);
    }

    @Nullable
    @Override
    public G add(@Nullable G left, @Nullable G right) {
        if (Converters.checkNull(left).perBucket.size() != Converters.checkNull(right).perBucket.size())
            throw new RuntimeException("Incompatible sizes for groups: " +
                    left.perBucket.size() + " and " + right.perBucket.size());
        R perMissing = this.missingSketch.add(left.perMissing, right.perMissing);
        JsonList<R> perBucket = Linq.zipMap(left.perBucket, right.perBucket,
                Linq.map(this.bucketSketch, s -> s::add));
        // R noBucket = this.sketch.add(left.outOfRange, right.outOfRange);
        return this.groupFactory.apply(perBucket, Converters.checkNull(perMissing)
                /*, Converters.checkNull(noBucket) */);
    }
}
