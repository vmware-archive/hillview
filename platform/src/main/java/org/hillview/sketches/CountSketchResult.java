package org.hillview.sketches;

import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.rows.RowSnapshot;

import java.io.Serializable;
import java.util.Arrays;

public class CountSketchResult implements Serializable {
    public CountSketchDescription csDesc;
    public long[][] counts;

    public CountSketchResult(CountSketchDescription csDesc) {
        this.csDesc= csDesc;
        this.counts = new long[csDesc.trials][csDesc.buckets];
    }

    public long[] getTrace(BaseRowSnapshot rss) {
        long item, hash;
        int sign, toBucket;
        long [] estimate = new long[this.csDesc.trials];
        item = rss.hashCode();
        for (int j = 0; j < this.csDesc.trials; j++) {
            hash = this.csDesc.hashFunction[j].hashLong(item);
            sign = (hash % 2 == 0) ? 1 : -1;
            toBucket = (int) (Math.abs(hash / 2) % this.csDesc.buckets);
            estimate[j] = this.counts[j][toBucket] * sign;
        }
        Arrays.sort(estimate);
        return estimate;
    }

    public long estimateFreq(BaseRowSnapshot rss) {
        return getTrace(rss)[this.csDesc.trials/2];
    }

    public double estimateNorm() {
        double[] estimate = new double[this.csDesc.trials];
        for (int i = 0; i < this.csDesc.trials; i++)
            for(int j = 0; j < this.csDesc.buckets; j++)
                estimate[i] += this.counts[i][j]*this.counts[i][j];
        Arrays.sort(estimate);
        return Math.sqrt(estimate[this.csDesc.trials/2]);
    }

    public long[][] getEstimates(RowSnapshot[] rss) {
        long item, hash;
        int sign, toBucket;
        long [][] estimate = new long[rss.length][this.csDesc.trials];
        for (int i = 0; i < rss.length; i++) {
            item = rss[i].hashCode();
            for (int j = 0; j < this.csDesc.trials; j++) {
                hash = this.csDesc.hashFunction[j].hashLong(item);
                sign = (hash % 2 == 0) ? 1 : -1;
                toBucket = (int) (Math.abs(hash / 2) % this.csDesc.buckets);
                estimate[i][j] = this.counts[j][toBucket] * sign;
            }
        }
        return estimate;
    }
}
