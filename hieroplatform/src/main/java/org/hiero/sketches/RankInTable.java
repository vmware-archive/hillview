package org.hiero.sketches;

import org.hiero.table.BaseRowSnapshot;
import org.hiero.table.RecordOrder;
import org.hiero.table.SmallTable;
import org.hiero.table.VirtualRowSnapshot;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;

public class RankInTable {
    private ITable table;
    private RecordOrder ro;

    public RankInTable(ITable table, RecordOrder ro) {
        this.table = table;
        this.ro = ro;
    }

    public int getRank(BaseRowSnapshot brs) {
        int rank = 0;
        IRowIterator rowIt = this.table.getRowIterator();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(this.table);
        int i = rowIt.getNextRow();
        while (i!= -1) {
            vrs.setRow(i);
            rank += ((brs.compareTo(vrs, ro) >= 0) ? 1: 0);
            i = rowIt.getNextRow();
        }
        return rank;
    }

    public int[] getRank(SmallTable st) {
        int [] rank = new int[st.getNumOfRows()];
        VirtualRowSnapshot vr = new VirtualRowSnapshot(st);
        for (int j =0; j < st.getNumOfRows(); j++) {
            vr.setRow(j);
            rank[j] = this.getRank(vr);
        }
        return rank;
    }
}
