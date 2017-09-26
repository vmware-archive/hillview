/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

/**
 * A helper method to test the quality of sorting based methods like quantiles.
 * It computes the rank of a rowSnapshot in a table under a specified ordering.
 **/
public class RankInTable {
    private final ITable table;
    private final RecordOrder ro;

    public RankInTable(ITable table, RecordOrder ro) {
        this.table = table;
        this.ro = ro;
    }

    /**
     * Given a rowSnapshot, compute its rank in a table order according to a given recordOrder.
     * @param brs The rowSnapshot whose rank we wish to compute.
     * @return Its rank in the table, which is the number of rows that are strictly smaller.
     */
    private int getRank(BaseRowSnapshot brs) {
        int rank = 0;
        IRowIterator rowIt = this.table.getRowIterator();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(this.table);
        int i = rowIt.getNextRow();
        while (i!= -1) {
            vrs.setRow(i);
            rank += ((brs.compareTo(vrs, this.ro) >= 0) ? 1: 0);
            i = rowIt.getNextRow();
        }
        return rank;
    }

    /**
     * Given a small table, compute the rank of each row in a large table order according to
     * a given recordOrder.
     * @param st The small table.
     * @return An integer array containing the rank of each row.
     */
    public int[] getRank(SmallTable st) {
        int [] rank = new int[st.getNumOfRows()];
        VirtualRowSnapshot vr = new VirtualRowSnapshot(st);
        for (int j =0; j < st.getNumOfRows(); j++) {
            vr.setRow(j);
            rank[j] = this.getRank(vr);
        }
        return rank;
    }
}
