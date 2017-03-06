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
 *
 */

package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntArrayColumn;
import org.hiero.sketch.table.ListComparator;
import org.hiero.sketch.table.StringArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IndexComparator;
import org.hiero.utils.Randomness;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.assertTrue;

public class ListComparatorTest {
    @Test
    public void ListComparatorZero() {
        final int size = 1000, maxRange = 100;
        final Randomness rn = Randomness.getInstance();
        final ColumnDescription desc1 = new ColumnDescription("test", ContentsKind.Int, false);
        final ColumnDescription desc2 = new ColumnDescription("test", ContentsKind.String, false);
        final IntArrayColumn col1 = new IntArrayColumn(desc1, size);
        final StringArrayColumn col2 = new StringArrayColumn(desc2, size);
        final List<IndexComparator> listCompare = new ArrayList<IndexComparator>();
        listCompare.add(col1.getComparator());
        listCompare.add(col2.getComparator().rev());
        final ListComparator listComp;
        listComp = new ListComparator(listCompare);
        final List<Integer> toSort = new ArrayList<Integer>();
        final String alphabet = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < size; i++) {
            col1.set(i, rn.nextInt(maxRange));
            col2.set(i, String.valueOf(alphabet.charAt(rn.nextInt(26))));
            toSort.add(i);
        }
        toSort.sort(listComp);
        for (int i = 0; i < (size - 1); i++) {
            assertTrue(listComp.compare(toSort.get(i), toSort.get(i + 1)) <= 0);
            assertTrue(col1.getComparator().compare(toSort.get(i), toSort.get(i + 1)) <= 0);
            if (col1.getComparator().compare(toSort.get(i), toSort.get(i + 1)) == 0) {
                assertTrue(col2.getComparator().compare(toSort.get(i), toSort.get(i + 1)) >= 0);
            }
        }
    }

    @Test
    public void ListComparatorTestOne() {
        final int size = 1000, maxRange = 8, numCols = 3;
        final Randomness rn = Randomness.getInstance();
        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, false);
        final ArrayList<IColumn> cols = new ArrayList<IColumn>(numCols);
        final List<IndexComparator> listCompare = new ArrayList<IndexComparator>();
        for(int i = 0; i < numCols; i++) {
            final IntArrayColumn newCol = new IntArrayColumn(desc, size);
            for (int j = 0; j < size; j++) {
                newCol.set(j, rn.nextInt(maxRange));
            }
            cols.add(newCol);
            listCompare.add(newCol.getComparator());
        }
        final ListComparator lComp = new ListComparator(listCompare);
        final List<Integer> toSort = new ArrayList<Integer>();
        for (int j = 0; j < size; j++) {
            toSort.add(j);
        }
        toSort.sort(lComp);
        String thisRow, nextRow;
        final RowsAsStrings rowString = new RowsAsStrings(cols);
        for(int i = 0; i < (size - 1); i++) {
            thisRow = rowString.getRow(toSort.get(i));
            nextRow = rowString.getRow(toSort.get(i+1));
            assertTrue(thisRow.compareTo(nextRow) <= 0);
        }
    }

    static class RowsAsStrings {
        private final ArrayList<IColumn> cols;

        public RowsAsStrings(final ArrayList<IColumn> cols){
            this.cols = cols;
        }

        public String getRow(final Integer rowIndex){
            String row = "";
            for(final IColumn thisCol: this.cols){
                row += (thisCol.asString(rowIndex) == null)? " " : thisCol.asString(rowIndex);
                row += " ";
            }
            return row;
        }
    }
}