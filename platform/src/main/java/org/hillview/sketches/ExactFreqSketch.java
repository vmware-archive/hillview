package org.hillview.sketches;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.BaseRowSnapshot;
import org.hillview.table.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;

/**
 * This sketch computes the true frequencies of a list of rowSnapshots in a data set. It can
 * be used right after the FreqKSketch which computes the list of heavy hitters, to compute their
 * exact frequencies.
 */
public class ExactFreqSketch implements ISketch<ITable, FreqKList> {

    /**
     * The schema of the RowSnapshots
     */
    private final Schema schema;
    /**
     * The set of RowSnapshots whose frequencies we wish to compute.
     */
    private final List<RowSnapshot> rssList;
    /**
     * The K in top top-K. Is used as a threshold to eliminate items that do not occur with
     * frequency 1/K.
     */
    private int maxSize;

    public ExactFreqSketch(Schema schema, FreqKList fk) {
        this.schema = schema;
        this.rssList = fk.getList();
        this.maxSize = fk.maxSize;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(this.rssList);
    }

    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        Converters.checkNull(left);
        Converters.checkNull(right);
        return left.add(right);
    }

    @Override
    public FreqKList create(ITable data) {
        HashingStrategy<BaseRowSnapshot> hs = new HashingStrategy<BaseRowSnapshot>() {
            @Override
            public int computeHashCode(BaseRowSnapshot brs) {
                if (brs instanceof VirtualRowSnapshot) {
                    return brs.hashCode();
                } else if (brs instanceof RowSnapshot) {
                    return brs.computeHashCode(ExactFreqSketch.this.schema);
                } else throw new RuntimeException("Uknown type encountered");
            }

            @Override
            public boolean equals(BaseRowSnapshot brs1, BaseRowSnapshot brs2) {
                return brs1.compareForEquality(brs2, ExactFreqSketch.this.schema);
            }
        };
        UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer> hMap = new
                UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer>(hs);
        this.rssList.forEach(rss -> hMap.put(rss, 0));
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.schema);
        while (i != -1) {
            vrs.setRow(i);
            if (hMap.containsKey(vrs)) {
                int count = hMap.get(vrs);
                hMap.put(vrs, count + 1);
            }
            i = rowIt.getNextRow();
        }
        HashMap<RowSnapshot, Integer> hm = new HashMap<RowSnapshot, Integer>(this.rssList.size());
        this.rssList.forEach(rss -> hm.put(rss, hMap.get(rss)));
        return new FreqKList(data.getNumOfRows(), this.maxSize, hm);
    }
}
