package org.hillview.sketches;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;
import java.util.ArrayList;
import java.util.List;

/**
 * A subclass of FreqKList that is used by the Exact Frequency Sketch. It maintains counts for a
 * fixed sets of RowSnapShots.
 */
public class FreqKListExact extends FreqKList {

    /**
     * The list of RowSnapShots whose frequencies we wish to compute.
     */
    List<RowSnapshot> rssList;

    static Object2IntOpenHashMap<RowSnapshot> buildHashMap(List<RowSnapshot> rssList) {
        Object2IntOpenHashMap<RowSnapshot> hMap = new Object2IntOpenHashMap<RowSnapshot>();
        rssList.forEach(rss -> hMap.put(rss, 0));
        return hMap;
    }

    public FreqKListExact(double epsilon, List<RowSnapshot> rssList) {
        super(0, epsilon, buildHashMap(rssList));
        this.rssList = rssList;
    }

    public FreqKListExact(long totalRows, double epsilon, Object2IntOpenHashMap<RowSnapshot> hMap,
                          List<RowSnapshot> rssList) {
        super(totalRows, epsilon, hMap);
        this.rssList = rssList;
    }

    /**
     * Since all counts computed by this sketch are exact, we discard everything less than epsilon
     * using filter and return the rest.
     */
<<<<<<< 63f5f32835edf0a4eadcc151a5bad9a0ef6430e0
    @Override
    public NextKList getTop(Schema schema) {
        this.filter();
        List <Pair<RowSnapshot, Integer>> pList = new ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        return getTopK(pList, schema);
    }
=======
    public FreqKListExact add(FreqKListExact that) {
        this.totalRows += that.totalRows;

        for (Object2IntMap.Entry<RowSnapshot> entry : this.hMap.object2IntEntrySet()) {
            int newVal = entry.getIntValue() + that.hMap.getOrDefault(entry.getKey(), 0);
            entry.setValue(newVal);
        }
        return this;
    }

    @Override
    public NextKList getTop(int size, Schema schema) {
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> {
            if (j >= this.epsilon * this.totalRows)
                pList.add(new Pair<RowSnapshot, Integer>(rs, j));
        });
        return getTopK(size, pList, schema);
    }

>>>>>>> Monday night
    public void filter() {
        double threshold = this.epsilon * this.totalRows;
        this.fkFilter(threshold);
    }
}