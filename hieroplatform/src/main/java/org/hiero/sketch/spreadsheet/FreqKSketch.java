package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.Pair;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FreqKSketch implements ISketch<ITable, FreqKList> {
    private final RecordOrder recordOrder;
    private final int maxSize;

    public FreqKSketch(List<ColumnDescription> colDescList, int maxSize) {
        this.recordOrder = new RecordOrder();
        colDescList.forEach(cd -> this.recordOrder.append(new ColumnSortOrientation(cd, true)));
        this.maxSize = maxSize;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.maxSize, new HashMap<>(0));
    }

    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        Converters.checkNull(left);
        for (RowSnapshot rs : Converters.checkNull(right).hMap.keySet()) {
            if (left.hMap.containsKey(rs)) {
                left.hMap.put(rs, left.hMap.get(rs) + right.hMap.get(rs));
            } else
                left.hMap.put(rs, right.hMap.get(rs));
        }
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<>(left.hMap.size());
        left.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(p2.second, p1.second));
        int k = 0;
        if(pList.size() >= (this.maxSize + 1)) {
            k = pList.get(this.maxSize).second;
        }
        HashMap<RowSnapshot,Integer> hm = new HashMap<>(this.maxSize);
        for (int i = 0; i < Math.min(this.maxSize, pList.size()); i++) {
            if (pList.get(i).second >= (k + 1))
                hm.put(pList.get(i).first, pList.get(i).second - k);
        }
        return new FreqKList(left.totalRows + right.totalRows, this.maxSize, hm);
    }

    @Override
    public FreqKList create(ITable data) {
        IRowIterator rowIt = data.getRowIterator();
        Schema schema = this.recordOrder.toSchema();
        HashMap <VirtualRowSnapshot, Integer> hMap = new HashMap<>(this.maxSize);
        List <VirtualRowSnapshot> toRemove = new ArrayList<>(this.maxSize);
        int i;
        do {
            i = rowIt.getNextRow();
            if (i != -1) {
                VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, i, schema);
                if (hMap.containsKey(vrs)) {
                    hMap.put(vrs, hMap.get(vrs) + 1);
                    }
                else if (hMap.size() < this.maxSize)
                    hMap.put(vrs, 1);
                else {
                    toRemove.clear();
                    for (VirtualRowSnapshot vr: hMap.keySet()) {
                        hMap.put(vr, hMap.get(vr) -1);
                        if(hMap.get(vr) == 0)
                            toRemove.add(vr);
                        }
                    toRemove.forEach(hMap::remove);
                    }
                }
            } while(i != -1);
        HashMap<RowSnapshot,Integer> hm = new HashMap<>(this.maxSize);
        for (VirtualRowSnapshot vrs : hMap.keySet()) {
            hm.put(vrs.materialize(), hMap.get(vrs));
        }
        return new FreqKList(data.getNumOfRows(), this.maxSize, hm);
    }
}
