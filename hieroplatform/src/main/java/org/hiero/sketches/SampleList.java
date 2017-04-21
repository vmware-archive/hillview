package org.hiero.sketches;

import org.hiero.table.ArrayRowOrder;
import org.hiero.table.RowSnapshot;
import org.hiero.table.SmallTable;

public class SampleList {
    /**
     * The quantiles are stored as rows of this table.
     */
    public SmallTable table;

    public SampleList(SmallTable table) {
        this.table = table;
    }

    /**
     * Returns the empirical p^th quantile
     * @param p
     * @return The p^th row in the sample
     */
    public RowSnapshot getRow(float p) {
        return new RowSnapshot(table, (int) p*this.table.getNumOfRows());
    }

    public SmallTable getQuantiles(int resolution) {
        if (this.table.getNumOfRows() < resolution + 1)
            return this.table;
        else {
            int[] order = new int[resolution];
            for (int i = 0; i < resolution; i++) {
                order[i] = Math.round((i + 1) * table.getNumOfRows() / (resolution + 1) - 1);
            }
            return this.table.compress(new ArrayRowOrder(order));
        }
    }


}
