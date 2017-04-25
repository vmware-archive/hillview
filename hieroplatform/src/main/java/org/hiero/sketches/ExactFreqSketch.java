package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.*;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * This sketch computes the true frequencies of a list of rowSnapshots in a data set. It can
 * be used right after the FreqKSketch which computes the list of heavy hitters, to compute their
 * exact frequencies.
 */
public class ExactFreqSketch implements ISketch<ITable, ExactFreqSketch.Frequencies> {
    public static class Frequencies {
        public final HashMap<BaseRowSnapshot, Integer> count;

        public Frequencies() { this.count = new HashMap<BaseRowSnapshot, Integer>(); }
        public Frequencies add(Frequencies to) {
            Frequencies result = new Frequencies();
            for (BaseRowSnapshot rs : this.count.keySet()) {
                int other = to.count.getOrDefault(rs, 0);
                result.count.put(rs, this.count.get(rs) + other);
            }
            for (BaseRowSnapshot rs: to.count.keySet()) {
                if (!result.count.containsKey(rs))
                    result.count.put(rs, to.count.get(rs));
            }
            return result;
        }
        public void addNew(BaseRowSnapshot rs, int count) {
            this.count.put(rs, count);
        }
    }

    /**
     * The schema of the RowSnapshots
     */
    private final Schema schema;
    /**
     * The set of RowSnapshots whose frequencies we wish to compute.
     */
    private final RowSnapshotSet rssList;

    public <T extends BaseRowSnapshot> ExactFreqSketch(Schema schema, Iterable<T> rssList) {
        this.schema = schema;
        this.rssList = new RowSnapshotSet(schema, rssList);
    }

    @Override
    public Frequencies zero() {
        return new Frequencies();
    }

    @Override
    public Frequencies add(@Nullable Frequencies left, @Nullable Frequencies right) {
        Converters.checkNull(left);
        Converters.checkNull(right);
        return left.add(right);
    }

    @Override
    public Frequencies create(ITable data) {
        HashMap<Integer, Integer> rowCounts = new HashMap<Integer, Integer>();
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.schema);
        while (i != -1) {
            vrs.setRow(i);
            if (this.rssList.contains(vrs)) {
                int count = rowCounts.getOrDefault(i, 0);
                rowCounts.put(i, count + 1);
            }
            i = rowIt.getNextRow();
        }
        Frequencies result = new Frequencies();
        for (Integer index : rowCounts.keySet()) {
            RowSnapshot rs = new RowSnapshot(data, index);
            result.addNew(rs, rowCounts.get(index));
        }
        return result;
    }
}