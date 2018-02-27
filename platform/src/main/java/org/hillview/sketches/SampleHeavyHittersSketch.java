package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.Schema;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;
import java.util.List;

<<<<<<< 9dc2740d9c27c4a12ee166e88accf367492a8d59
/**
 * A heavy hitters sketch where we sample each row in the database with a certain probability, and
 * then compute heavy hitters over this sample. Provided the sample size is O(1/epsilon^2), we
 * expect every element with relative frequency at least epsilon to appear in the sample with
 * approximately the right relative frequency.
 */
=======
>>>>>>> Refactored FreqKList
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
     * The size of the input table.
     */
    private final long totalRows;
    /**
     * The rate at which we sample data.
     */
    private final double samplingRate;
    final long seed;

    public SampleHeavyHittersSketch(Schema schema, double epsilon, long totalRows, long seed) {
        this.schema = schema;
        this.epsilon = epsilon;
        this.totalRows= totalRows;
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

<<<<<<< 9dc2740d9c27c4a12ee166e88accf367492a8d59
    /**
     * Add takes the union of the two lists, adding the frequencies for any common
     * elements.
     */
    public FreqKListSample add(@Nullable FreqKListSample left, @Nullable FreqKListSample right) {
        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList =
                FreqKList.addLists(left, right);
=======
    public FreqKListSample add(@Nullable FreqKListSample left, @Nullable FreqKListSample right) {
        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList = FreqKList.addLists(left, right);
>>>>>>> Refactored FreqKList
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(pList.size());
        for (Object2ObjectMap.Entry<RowSnapshot, MutableInteger> aPList : pList) {
            hm.put(aPList.getKey(), aPList.getValue().get());
        }
        return new FreqKListSample(left.totalRows + right.totalRows, this.epsilon,
                left.sampleSize + right.sampleSize, hm);
    }

<<<<<<< 9dc2740d9c27c4a12ee166e88accf367492a8d59
    /**
     * Create computes a histogram of RowSnapShots over the sample.
     */
=======
>>>>>>> Refactored FreqKList
    public FreqKListSample create(ITable data) {
        IntHash.Strategy hs = new IntHash.Strategy() {
            final VirtualRowSnapshot vrs = new VirtualRowSnapshot(data,
                    SampleHeavyHittersSketch.this.schema);
            final VirtualRowSnapshot vrs1 = new VirtualRowSnapshot(data,
                    SampleHeavyHittersSketch.this.schema);

            @Override
            public int hashCode(int index) {
                this.vrs.setRow(index);
                return this.vrs.computeHashCode(SampleHeavyHittersSketch.this.schema);
            }

            @Override
            public boolean equals(int index, int otherIndex) {
                this.vrs.setRow(index);
                this.vrs1.setRow(otherIndex);
                return this.vrs.compareForEquality(this.vrs1, SampleHeavyHittersSketch.this.schema);
            }
        };

        Int2ObjectOpenCustomHashMap<MutableInteger> hMap = new Int2ObjectOpenCustomHashMap<MutableInteger>(hs);
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
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(hMap.size());
        for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>> it =
             hMap.int2ObjectEntrySet().fastIterator(); it.hasNext(); ) {
            final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
            hm.put(new RowSnapshot(data, entry.getIntKey(), this.schema), entry.getValue().get());
        }
        return new FreqKListSample(data.getNumOfRows(), this.epsilon, sampleSet.getSize(), hm);
    }
}
