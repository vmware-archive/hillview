package org.hillview.sketches;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;

import java.util.ArrayList;
import java.util.List;

public class FreqKListMG extends FreqKList {

    /**
     * In MG it is the number of counters we store: the K in top-K heavy hitters.
     **/
    public final int maxSize;

    public FreqKListMG(long totalRows, double epsilon, int maxSize, Object2IntOpenHashMap<RowSnapshot> hMap) {
        super(totalRows, epsilon, hMap);
        this.maxSize = maxSize;
    }

    /**
     * This method returns the sum of counts computed by the data structure. This is always less
     * than totalRows, the number of rows in the table.
     *
     * @return The sum of all counts stored in the hash-map.
     */
    int getTotalCount() {
        return this.hMap.values().stream().reduce(0, Integer::sum);
    }

    /**
     * The error bound guaranteed by the "Mergeable Summaries" paper. It holds if a particular
     * sketch algorithm is applied to the Misra-Gries map. In particular, if the sum of observed
     * frequencies equals the total length of the table, the error is zero. Two notable properties
     * of this error bound:
     * - The frequency f(i) in the table is always an underestimate
     * - The true frequency lies between f(i) and f(i) + e, where e is the bound returned below.
     *
     * @return Integer e such that if an element i has a count f(i) in the data
     * structure, then its true frequency in the range [f(i), f(i) +e].
     */
    public double getErrBound() {
        return (this.totalRows - this.getTotalCount()) / (this.maxSize + 1.0);
    }

    public void filter() {
        double threshold = this.epsilon * this.totalRows - this.getErrBound();
        this.fkFilter(threshold);
    }

    /**
     * We return all elements whose fractional counts that could possibly be above epsilon, using
     * the error bound calculation in getErrBound()
     */
    @Override
    public NextKList getTop(Schema schema) {
        this.filter();
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> { pList.add(new Pair<RowSnapshot, Integer>(rs, j));});
        return getTopK(pList, schema);
    }
}
