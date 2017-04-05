package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.dataset.api.Pair;
import org.hiero.table.*;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Computes heavy-hitters using the Misra-Gries algorithm, where N is the length on the input
 * table, and k is the number of counters that we maintain. We use the mergeable version of MG, as
 * described in the ACM TODS paper "Mergeable Summaries" by Agarwal et al., which gives a
 * non-trivial error bound. The algorithm ensures that every element of frequency greater than
 * N/(k +1) appears in the list.
 */
public class FreqKSketch implements ISketch<ITable, FreqKList> {
    /**
     * The schema specifies which columns are relevant in determining equality of records.
     */
    private final Schema schema;
    /**
     * The parameter K in top-K heavy hitters.
     */
    private final int maxSize;

    public FreqKSketch(Schema schema, int maxSize) {
        this.schema = schema;
        this.maxSize = maxSize;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.maxSize, new HashMap<RowSnapshot, Integer>(0));
    }

    /**
     * The add procedure as specified by Agarwal et al. (Mergeable Summaries, TODS).
     * @param left The first MG sketch.
     * @param right The second MG sketch.
     * @return The merged sketch, where we first add the frequency vectors, and then subtract the
     * (k+1)^th frequency from the top k. This guarantees a strong error bound.
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        for (RowSnapshot rs : right.hMap.keySet()) {
            if (left.hMap.containsKey(rs)) {
                left.hMap.put(rs, left.hMap.get(rs) + right.hMap.get(rs));
            } else
                left.hMap.put(rs, right.hMap.get(rs));
        }
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<>(left.hMap.size());
        left.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(p2.second, p1.second));
        int k = 0;
        if (pList.size() >= (this.maxSize + 1)) {
            k = pList.get(this.maxSize).second;
        }
        HashMap<RowSnapshot,Integer> hm = new HashMap<RowSnapshot, Integer>(this.maxSize);
        for (int i = 0; i < Math.min(this.maxSize, pList.size()); i++) {
            if (pList.get(i).second >= (k + 1))
                hm.put(pList.get(i).first, pList.get(i).second - k);
        }
        return new FreqKList(left.totalRows + right.totalRows, this.maxSize, hm);
    }

    /**
     * Creates the MG sketch, by the Misra-Gries algorithm.
     * @param data  Data to sketch.
     * @return A FreqKList.
     */
    @Override
    public FreqKList create(ITable data) {
        IRowIterator rowIt = data.getRowIterator();
        HashMap<Integer, Integer> hMap = new
                HashMap<Integer, Integer>(this.maxSize);
        List<Integer> toRemove = new ArrayList<Integer>(this.maxSize);
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.schema);
        while (i != -1) {
            vrs.setRow(i);
            if (hMap.containsKey(i)) {
                hMap.put(i, hMap.get(i) + 1);
            } else if (hMap.size() < this.maxSize)
                hMap.put(i, 1);
            else {
                toRemove.clear();
                for (Integer vr : hMap.keySet()) {
                    hMap.put(vr, hMap.get(vr) - 1);
                    if (hMap.get(vr) == 0)
                        toRemove.add(i);
                }
                toRemove.forEach(hMap::remove);
            }
            i = rowIt.getNextRow();
        }
        HashMap<RowSnapshot,Integer> hm = new HashMap<RowSnapshot, Integer>(this.maxSize);
        hMap.keySet().forEach(ri -> hm.put(new RowSnapshot(data, ri), hMap.get(ri)));
        return new FreqKList(data.getNumOfRows(), this.maxSize, hm);
    }
}
