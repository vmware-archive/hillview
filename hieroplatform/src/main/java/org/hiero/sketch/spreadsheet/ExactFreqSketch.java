package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.RecordEq;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.VirtualRowSnapshot;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ITable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;

public class ExactFreqSketch implements ISketch<ITable, HashMap<RowSnapshot, Integer>> {
    private List<RowSnapshot> rssList;
    private RecordEq rEq;

    public ExactFreqSketch(List<RowSnapshot> rssList, RecordEq rEq) {
        this.rssList = rssList;
        this.rEq = rEq;
    }

    @Nullable
    @Override
    public HashMap<RowSnapshot, Integer> zero() {
        HashMap<RowSnapshot, Integer> hMap = new HashMap<>();
        this.rssList.forEach(t -> hMap.put(t, 0));
        return hMap;
    }

    @Nullable
    @Override
    public HashMap<RowSnapshot, Integer> add(@Nullable HashMap<RowSnapshot, Integer> left,
                                             @Nullable HashMap<RowSnapshot, Integer> right) {
        return null;
    }

    @Override
    public HashMap<RowSnapshot, Integer> create(ITable data) {
        HashMap<RowSnapshot, Integer> hMap = new HashMap<>();
        this.rssList.forEach(t -> hMap.put(t, 0));
        IRowIterator rowIt = data.getRowIterator();
        int i;
        VirtualRowSnapshot vrs;
        do{
            i = rowIt.getNextRow();
            if(i != -1)
                vrs = new VirtualRowSnapshot(data, i, rEq.toSchema());
        }
        while (i != -1);
        return hMap;
    }
}
