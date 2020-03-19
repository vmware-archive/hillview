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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;

/**
 * This compute the Misra-Gries sketch for finding Heavy Hitters.
 */
public class FreqKListMG extends FreqKList {
    static final long serialVersionUID = 1;
    
    /**
     * The number of counters we store, it is at least 1/epsilon, if the goal is to find all
     * elements with relative frequency epsilon. Increasing K increases the accuracy of the
     * approximate counts.
     **/
    private final int maxSize;

    public FreqKListMG(long totalRows, double epsilon, int maxSize, Object2IntOpenHashMap<RowSnapshot> hMap) {
        super(totalRows, epsilon, hMap);
        this.maxSize = maxSize;
    }

    /**
     * This method returns the sum of counts computed by the data structure. This is always less
     * than rowsScanned, the number of rows in the table.
     * @return The sum of all counts stored in the hash-map.
     */
    private int getTotalCount() {
        return this.hMap.values().stream().reduce(0, Integer::sum);
    }

    /**
     * The error bound guaranteed by the "Mergeable Summaries" paper. It holds if a particular
     * sketch algorithm is applied to the Misra-Gries map. In particular, if the sum of observed
     * frequencies equals the total length of the table, the error is zero. Two notable properties
     * of this error bound:
     * - The frequency f(i) in the table is always an underestimate
     * - The true frequency lies between f(i) and f(i) + e, where e is the bound returned below.
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
        this.hMap.forEach((rs, j) -> this.pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        return this.sortTopK(schema);
    }
}
