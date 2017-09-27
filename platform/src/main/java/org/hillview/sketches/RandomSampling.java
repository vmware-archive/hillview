package org.hillview.sketches;

import org.hillview.table.SmallTable;
import org.hillview.table.rows.RowSnapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class RandomSampling implements Serializable {

    public final SmallTable table;
    private int numSamples;
    private long numRows;

    public RandomSampling(SmallTable table, int numSamples, long numRows) {
        this.table = table;
        this.numSamples = numSamples;
        this.numRows = numRows;
    }

    // TODO: Think about what happens if a node has fewer than numSamples rows.
    public RandomSampling union(RandomSampling that) {
        if (this.numSamples != that.numSamples)
            throw new RuntimeException("Expected number of samples from both RandomSamplings to be equal.");

        if (this.numRows == 0)
            return new RandomSampling(that.table, that.numSamples, that.numRows);
        else if (that.numRows == 0)
            return new RandomSampling(this.table, this.numSamples, this.numRows);

        // Number of samples from this's sampling
        int alpha = (int) Math.round(this.numSamples * ((double) this.numRows) / (this.numRows + that.numRows));
        // Number of samples from that's sampling
        int beta = this.numSamples - alpha;

        SmallTable thisSample = this.table.compress(this.table.getMembershipSet().sample(alpha));
        SmallTable thatSample = that.table.compress(that.table.getMembershipSet().sample(beta));

        List<RowSnapshot> rows = new ArrayList<RowSnapshot>(thisSample.getNumOfRows() + thatSample.getNumOfRows());
        for (int i = 0; i < thisSample.getNumOfRows(); i++)
            rows.add(new RowSnapshot(thisSample, i));
        for (int i = 0; i < thatSample.getNumOfRows(); i++)
            rows.add(new RowSnapshot(thatSample, i));
        SmallTable newTable = new SmallTable(thisSample.getSchema(), rows);

        return new RandomSampling(newTable, this.numSamples, this.numRows + that.numRows);
    }
}
