package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.dataset.api.Pair;
import org.hiero.sketch.table.RowSnapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FreqKList implements Serializable, IJson {
    /**
     * The size of the input table.
     */
    public final long totalRows;
    /**
     * The number of counters we store: the K in top-K heavy hitters.
     */
    public final int maxSize;
    /**
     * Estimate for the number of times each row in the above table occurs in the original DataSet.
     */
    public final HashMap<RowSnapshot, Integer> hMap;

    public FreqKList(long totalRows, int maxSize, HashMap<RowSnapshot, Integer> hMap) {
        this.totalRows = totalRows;
        this.maxSize = maxSize;
        this.hMap = hMap;
    }

    public int GetTotalCount() {
        return this.hMap.values().stream().reduce(0, Integer::sum);
    }

    public int GetErrBound() {
        return (int) Math.ceil((this.totalRows - this.GetTotalCount())/(this.maxSize + 1.0));
    }

    public String toLongString() {
        List<Pair<RowSnapshot, Integer>> pList = new ArrayList<>(this.hMap.size());
        this.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(p2.second, p1.second));
        final StringBuilder builder = new StringBuilder();
        pList.forEach(p ->  builder.append(p.first.toString()).append(": ( ").append(p.second)
                                   .append(",").append(p.second + GetErrBound())
                                   .append(" )").append(System.getProperty("line.separator")));
        builder.append("Error bound: ").append(this.GetErrBound())
               .append(System.getProperty("line.separator"));
        return builder.toString();
    }
}