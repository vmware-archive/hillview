package org.hillview.sketches;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;

/**
 * A subclass of FreqKList that is used by the Sample Heavy Hitters Sketch.
 */
public class FreqKListSample extends FreqKList {
    /**
     * In sampleHeavyHitters, it is used to store the number of samples taken.
     */
    final int sampleSize;

    FreqKListSample(long totalRows, double epsilon, int sampleSize,
                           Object2IntOpenHashMap<RowSnapshot> hMap) {
        super(totalRows, epsilon, hMap);
        this.sampleSize = sampleSize;
    }

    /**
     * Since counts are approximate, we keep all elements that are have observed relative
     * frequencies above 0.5*epsilon, but only report those that are above epsilon.
     */
    @Override
    public NextKList getTop(Schema schema) {
        /* Needed here because this list is used for further filtering*/
        this.fkFilter(0.5 * epsilon * this.sampleSize);
        this.hMap.forEach((rs, j) -> {
            double fraction = ((double) j) / this.sampleSize;
            if (fraction >= epsilon) {
                int k = (int) (fraction * this.totalRows);
                this.pList.add(new Pair<RowSnapshot, Integer>(rs, k));
            }
        });
        return sortTopK(schema);
    }

    public void rescale() {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it = this.hMap.object2IntEntrySet().
                fastIterator(); it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            this.hMap.put(entry.getKey(), (int) (entry.getIntValue() *
                    ((double) this.totalRows)/this.sampleSize));
        }
    }
}
