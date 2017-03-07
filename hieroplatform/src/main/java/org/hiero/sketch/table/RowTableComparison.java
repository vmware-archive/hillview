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

package org.hiero.sketch.table;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;

/**
 * This class lets us compare a RowSnapShot to entries from a Table according to a prescribed
 * RecordOrder. The schema of the snapshot should be a subset of that of the Table.
 * This method is used to generate the NextK items in some order starting at a prescribed row by
 * NextKSketch.
 */
public class RowTableComparison {
    @Nullable
    private final RowSnapshot topRow;
    private final ITable table;
    private final RecordOrder recordOrder;

    public RowTableComparison(@Nullable RowSnapshot topRow, ITable table, RecordOrder recordOrder ) {
        this.topRow = topRow;
        this.table = table;
        this.recordOrder = recordOrder;
    }

    /**
     * Compares Row i of Table to topRow according to recordOrder.
     * @param i Index of a row in the Table.
     * @return 1 if row i is greater, 0 if they are equal, -1 if it is less.
     */
    public int compareToRow(int i) {
        int outcome = 0;
        if (this.topRow == null) return 1;
        for (ColumnSortOrientation ordCol : this.recordOrder) {
            String colName = ordCol.columnDescription.name;
            IColumn iCol = this.table.getColumn(colName);
            if (iCol.isMissing(i) && this.topRow.isMissing(colName)) {
                outcome = 0;
            } else if (iCol.isMissing(i)) {
                outcome = 1;
            } else if (this.topRow.isMissing(colName)) {
                outcome = -1;
            } else {
                switch (this.table.getSchema().getKind(colName)) {
                    case String:
                    case Json:
                        outcome = Converters.checkNull(iCol.getString(i))
                                            .compareTo(Converters.checkNull(this.topRow.getString(colName)));
                        break;
                    case Date:
                        outcome = Converters.checkNull(iCol.getDate(i)).compareTo(
                                Converters.checkNull(this.topRow.getDate(colName)));
                        break;
                    case Int:
                        outcome = Integer.compare(Converters.checkNull(iCol.getInt(i)),
                                Converters.checkNull(this.topRow.getInt(colName)));
                        break;
                    case Double:
                        outcome = Double.compare(Converters.checkNull(iCol.getDouble(i)),
                                Converters.checkNull(this.topRow.getDouble(colName)));
                        break;
                    case Duration:
                        outcome = Converters.checkNull(iCol.getDuration(i))
                                            .compareTo(Converters.checkNull(this.topRow.getDuration(colName)));
                        break;
                }
            }
            if (!ordCol.isAscending) {
                outcome *= -1;
            }
            if (outcome != 0) {
                return outcome;
            }
        }
        return 0;
    }
}