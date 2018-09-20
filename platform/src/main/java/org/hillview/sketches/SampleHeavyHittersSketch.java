package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.Schema;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowHashStrategy;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;

import static org.hillview.sketches.FreqKList.getUnion;

/**
 * A heavy hitters sketch where we sample each row in the database with a certain probability, and
 * then compute heavy hitters over this sample. Provided the sample size is O(1/epsilon^2), we
 * expect every element with relative frequency at least epsilon to appear in the sample with
 * approximately the right relative frequency.
 */
public class SampleHeavyHittersSketch implements ISketch<ITable, FreqKListSample> {
    /**
     * The schema specifies which columns are relevant in determining equality of records.
     */
    private final Schema schema;
    /**
     * epsilon specifies the threshold for the fractional frequency: our goal is to find all
     * elements whose relative frequencies are at least an epsilon fraction of the total.
     */
    private final double epsilon;
    /**
     * The rate at which we sample data.
     */
    private final double samplingRate;
    private final long seed;

    public SampleHeavyHittersSketch(Schema schema, double epsilon, long totalRows, long seed) {
        this.schema = schema;
        this.epsilon = epsilon;
        this.seed = seed;
        this.samplingRate = Math.min(1, Math.max(10.0/(totalRows*epsilon*epsilon),
                20000.0/totalRows));
    }

    @Nullable
    @Override
    public FreqKListSample zero() {
        return new FreqKListSample(0, this.epsilon, 0,
                new Object2IntOpenHashMap<RowSnapshot>(0));
    }

    /**
     * Add takes the union of the two lists, adding the frequencies for any common
     * elements.
     */
    public FreqKListSample add(@Nullable FreqKListSample left, @Nullable FreqKListSample right) {
        return new FreqKListSample(left.totalRows + right.totalRows, this.epsilon,
                left.sampleSize + right.sampleSize, getUnion(left, right));
    }

    /**
     * Create computes a histogram of RowSnapShots over the sample.
     */
    public FreqKListSample create(ITable data) {
        VirtualRowHashStrategy hashStrategy = new VirtualRowHashStrategy(data, this.schema);
        Int2ObjectOpenCustomHashMap<MutableInteger> hMap = new Int2ObjectOpenCustomHashMap<MutableInteger>(hashStrategy);
        final IMembershipSet sampleSet = data.
                getMembershipSet().sample(this.samplingRate, this.seed);
        IRowIterator rowIt = sampleSet.getIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            MutableInteger val = hMap.get(i);
            if (val != null) {
                val.set(val.get() + 1);
            } else {
                hMap.put(i, new MutableInteger(1));
            }
            i = rowIt.getNextRow();
        }
        Object2IntOpenHashMap<RowSnapshot> hm = hashStrategy.materializeHashMap(hMap);
        return new FreqKListSample(data.getNumOfRows(), this.epsilon, sampleSet.getSize(), hm);
    }
}
