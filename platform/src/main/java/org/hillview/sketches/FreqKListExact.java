package org.hillview.sketches;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;

import java.util.ArrayList;
import java.util.List;

public class FreqKListExact extends FreqKList {

    static Object2IntOpenHashMap<RowSnapshot> buildHashMap(List<RowSnapshot> rssList) {
        Object2IntOpenHashMap<RowSnapshot> hMap = new Object2IntOpenHashMap<RowSnapshot>();
        rssList.forEach(rss -> hMap.put(rss, 0));
        return hMap;
    }

    public FreqKListExact(long totalRows, List<RowSnapshot> rssList, double epsilon) {
        super(totalRows, epsilon, buildHashMap(rssList));
    }

    public FreqKListExact(List<RowSnapshot> rssList, double epsilon) {
        super(0, epsilon, buildHashMap(rssList));
    }

    public FreqKListExact(long totalRows, double epsilon, Object2IntOpenHashMap<RowSnapshot> hMap) {
        super(totalRows, epsilon, hMap);
    }

    /**
     * Used to add two Lists that have counts for the same set of RowSnapShots. Behavior is not
     * determined if it is called with two lists that have different sets of keys. Meant to be used
     * by ExactFreqSketch.
     *
     * @param that The list to be added to the current one.
     * @return Updated counts (existing counts are overwritten).
     */
    public FreqKListExact add(FreqKListExact that) {
        this.totalRows += that.totalRows;

        for (Object2IntMap.Entry<RowSnapshot> entry : this.hMap.object2IntEntrySet()) {
            int newVal = entry.getIntValue() + that.hMap.getOrDefault(entry.getKey(), 0);
            entry.setValue(newVal);
        }
        return this;
    }

    public NextKList getTop(int size, Schema schema) {
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> {
            if (j >= this.epsilon * this.totalRows)
                pList.add(new Pair<RowSnapshot, Integer>(rs, j));
        });
        return getTopK(size, pList, schema);
    }

    public NextKList getTop(Schema schema) {
        return getTop(this.hMap.size(), schema);
    }

    public void filter() {
        double threshold = this.epsilon * this.totalRows;
        this.fkFilter(threshold);
    }
}
