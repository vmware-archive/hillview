/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.sketches;

import org.hillview.table.ArrayRowOrder;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.SmallTable;

import java.io.Serializable;

/**
 * A sample of rows from a large table, stored in a small table. The expectation is that the rows
 * are sorted according to some order (this is needed for the getRow method to be meaningful).
 */
public class SampleList implements Serializable {
    /**
     * The table containing the rows.
     */
    public final SmallTable table;

    public SampleList(SmallTable table) {
        this.table = table;
    }

    /**
     * @param q in (0,1), which is the desired quantile.
     * @return Assuming the rows are sorted, this method returns the empirical p^th quantile as an
     * estimator for the p^th quantile in the large table.
     */
    public RowSnapshot getRow(double q) {
        return new RowSnapshot(this.table, (int) (q*this.table.getNumOfRows()));
    }

    /** A method that can be used in sketching to estimate the quality of the quantiles sketch.
     * @param resolution The desired number of rows.
     * @return Equally spaced rows from the sample table.
     */
    public SmallTable getQuantiles(int resolution) {
        if (this.table.getNumOfRows() < (resolution + 1))
            return this.table;
        else {
            int[] order = new int[resolution];
            for (int i = 0; i < resolution; i++) {
                order[i] = Math.round((((i + 1) * this.table.getNumOfRows()) / (resolution + 1)) - 1);
            }
            return this.table.compress(new ArrayRowOrder(order));
        }
    }
}
