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

import java.util.List;

/**
 * A subclass of FreqKList that is used by the Exact Frequency Sketch. It maintains counts for a
 * fixed sets of RowSnapShots.
 */
public class FreqKListExact extends FreqKList {
    static final long serialVersionUID = 1;    

    /**
     * The list of RowSnapShots whose frequencies we wish to compute.
     */
    public final List<RowSnapshot> rssList;

    private static Object2IntOpenHashMap<RowSnapshot> buildHashMap(List<RowSnapshot> rssList) {
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
    public void filter() {
        double threshold = this.epsilon * this.totalRows;
        this.fkFilter(threshold);
    }

    @Override
    public NextKList getTop(Schema schema) {
        this.filter();
        this.hMap.forEach((rs, j) -> this.pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        return this.sortTopK(schema);
    }
}
