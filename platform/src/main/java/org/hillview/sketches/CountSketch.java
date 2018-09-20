package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;

public class CountSketch implements ISketch<ITable, CountSketchResult> {
    public CountSketchDescription csDesc;

    public CountSketch(CountSketchDescription csDesc) {
        this.csDesc = csDesc;
    }

    @Override
    public CountSketchResult create(ITable data) {
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.csDesc.schema);
        CountSketchResult result = new CountSketchResult(this.csDesc);
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        long item, hash;
        int sign, toBucket;
        while (i != -1) {
            vrs.setRow(i);
            item = vrs.hashCode();
            for (int j = 0; j < this.csDesc.trials; j++) {
                hash = this.csDesc.hashFunction[j].hashLong(item);
                sign = (hash % 2 == 0)? 1: -1;
                toBucket = (int) (Math.abs(hash/2) % this.csDesc.buckets);
                result.counts[j][toBucket] += sign;
            }
            i = rowIt.getNextRow();
        }
        return result;
    }

    @Nullable
    @Override
    public CountSketchResult zero() {
        return new CountSketchResult(this.csDesc);
    }

    @Nullable
    @Override
    public CountSketchResult add(@Nullable CountSketchResult left, @Nullable CountSketchResult right) {
        CountSketchResult sum = new CountSketchResult(this.csDesc);
        for (int i = 0; i < this.csDesc.trials; i++)
            for (int j = 0; j <  this.csDesc.buckets; j++)
                sum.counts[i][j] = left.counts[i][j] + right.counts[i][j];
        return sum;
    }


}
