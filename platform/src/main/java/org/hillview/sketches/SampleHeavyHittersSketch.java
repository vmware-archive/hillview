package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.objects.*;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.Schema;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SampleHeavyHittersSketch implements ISketch<ITable, FreqKList> {
    /**
     * The schema specifies which columns are relevant in determining equality of records.
     */
    private final Schema schema;

    /**
     * epsilon specifies the threshold for the fractional frequency: our goal is to find all elements
     * that constitute more than an epsilon fraction of the total.
     */
    private final double epsilon;

    /**
     * The size of the input table.
     */
    private long totalRows;

    /**
     * The rate at which we sample data.
     */
    private double samplingRate;
    final long seed;

    public SampleHeavyHittersSketch(Schema schema, double epsilon, long totalRows, long seed) {
        this.schema = schema;
        this.epsilon = epsilon;
        this.totalRows= totalRows;
        this.seed = seed;
        this.samplingRate = Math.max(1/(totalRows*epsilon*epsilon), 20000.0/totalRows);
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.epsilon, 0, new Object2IntOpenHashMap<RowSnapshot>(0));
    }

    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        Object2ObjectOpenHashMap<RowSnapshot, MutableInteger> resultMap =
                new Object2ObjectOpenHashMap<RowSnapshot, MutableInteger>(left.hMap.size() + right.hMap.size());
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = left.hMap.object2IntEntrySet().fastIterator();
             it1.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> it = it1.next();
            resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
        }

        // Add values of right.hMap to resultMap
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = right.hMap.object2IntEntrySet().fastIterator();
             it1.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> it = it1.next();
            MutableInteger val = resultMap.get(it.getKey());
            if (val != null) {
                val.set(val.get() + it.getIntValue());
            } else {
                resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
            }
        }

        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList =
                new ArrayList<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>>(resultMap.size());
        pList.addAll(resultMap.object2ObjectEntrySet());
        pList.sort((p1, p2) -> Integer.compare(p2.getValue().get(), p1.getValue().get()));

        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(pList.size());
        for (int i = 0; i < pList.size(); i++) {
            hm.put(pList.get(i).getKey(), pList.get(i).getValue().get());
        }
        return new FreqKList(left.totalRows + right.totalRows, this.epsilon,
                left.maxSize + right.maxSize, hm);
    }

    public FreqKList create(ITable data) {
        IntHash.Strategy hs = new IntHash.Strategy() {
            final VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, SampleHeavyHittersSketch.this.schema);
            final VirtualRowSnapshot vrs1 = new VirtualRowSnapshot(data, SampleHeavyHittersSketch.this.schema);

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
        final IMembershipSet sampleSet = data.getMembershipSet().sample(this.samplingRate, this.seed);
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
        for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>> it = hMap.int2ObjectEntrySet().fastIterator();
             it.hasNext(); ) {
            final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
            hm.put(new RowSnapshot(data, entry.getIntKey(), this.schema), entry.getValue().get());
        }
        return new FreqKList(data.getNumOfRows(), this.epsilon, sampleSet.getSize(), hm);
    }
}
