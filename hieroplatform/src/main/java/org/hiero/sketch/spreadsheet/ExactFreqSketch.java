package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.VirtualRowSnapshot;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ITable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;

/**
 * This sketch computes the true frequencies of a list of rowSnapshots in a data set. It can
 * be used right after the FreqKSketch which computes the list of heavy hitters, to compute their
 * exact frequencies.
 */
public class ExactFreqSketch implements ISketch<ITable, HashMap<RowSnapshot, Integer>> {
    /**
     * The schema of the RowSnapshots
     */
    private Schema schema;
    /**
     * The list of RowSnapshots whose frequencies we wish to compute.
     */
    private List<RowSnapshot> rssList;


    public ExactFreqSketch(Schema schema, List<RowSnapshot> rssList) {
        this.schema = schema;
        this.rssList = rssList;
    }

    @Override
    public HashMap<RowSnapshot, Integer> zero() {
        HashMap<RowSnapshot, Integer> hMap = new HashMap<>();
        this.rssList.forEach(t -> hMap.put(t, 0));
        return hMap;
    }

    @Override
    public HashMap<RowSnapshot, Integer> add(@Nullable HashMap<RowSnapshot, Integer> left,
                                             @Nullable HashMap<RowSnapshot, Integer> right) {
        this.rssList.forEach(rss -> left.put(rss, left.get(rss) + right.get(rss)));
        return left;
    }

    @Override
    public HashMap<RowSnapshot, Integer> create(ITable data) {
        HashMap<RowSnapshot, Integer> hMap = this.zero();
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs;
        while (i != -1) {
            vrs = new VirtualRowSnapshot(data, i, this.schema);
            this.rssList.stream().filter(vrs::equals)
                        .forEach(rss -> hMap.put(rss, hMap.get(rss) + 1));
            i = rowIt.getNextRow();
        }
        return hMap;
    }
}