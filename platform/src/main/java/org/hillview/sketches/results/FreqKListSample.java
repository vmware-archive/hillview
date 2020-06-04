/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches.results;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.utils.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;

/**
 * A subclass of FreqKList that is used by the Sample Heavy Hitters Sketch.
 */
public class FreqKListSample extends FreqKList {
    static final long serialVersionUID = 1;
    
    /**
     * In sampleHeavyHitters, it is used to store the number of samples taken.
     */
    public final int sampleSize;

    public FreqKListSample(long totalRows, double epsilon, int sampleSize,
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
                int k = Converters.toInt(fraction * this.totalRows);
                this.pList.add(new Pair<RowSnapshot, Integer>(rs, k));
            }
        });
        return sortTopK(schema);
    }

    public void rescale() {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it = this.hMap.object2IntEntrySet().
                fastIterator(); it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            this.hMap.put(entry.getKey(), Converters.toInt(entry.getIntValue() *
                    ((double) this.totalRows)/this.sampleSize));
        }
    }
}
