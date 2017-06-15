package org.hillview.utils;

import org.hillview.table.BaseRowSnapshot;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

/**
 * A helper method to test the quality of sorting based methods like quantiles.
 * It computes the rank of a rowSnapshot in a table under a specified ordering.
 **/
public class RankInTable {
    private final ITable table;
    private final RecordOrder ro;

    public RankInTable(ITable table, RecordOrder ro) {
        this.table = table;
        this.ro = ro;
    }

    /**
     * Given a rowSnapshot, compute its rank in a table order according to a given recordOrder.
     * @param brs The rowSnapshot whose rank we wish to compute.
     * @return Its rank in the table, which is the number of rows that are strictly smaller.
     */
    private int getRank(BaseRowSnapshot brs) {
        int rank = 0;
        IRowIterator rowIt = this.table.getRowIterator();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(this.table);
        int i = rowIt.getNextRow();
        while (i!= -1) {
            vrs.setRow(i);
            rank += ((brs.compareTo(vrs, this.ro) >= 0) ? 1: 0);
            i = rowIt.getNextRow();
        }
        return rank;
    }

    /**
     * Given a small table, compute the rank of each row in a large table order according to
     * a given recordOrder.
     * @param st The small table.
     * @return An integer array containing the rank of each row.
     */
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
