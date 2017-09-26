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

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

public class TestUtils {
    /**
     * Prints a table to stdout.
     * @param table The table that is printed.
     */
    public static void printTable(ITable table) {
        StringBuilder header = new StringBuilder("| ");
        for (IColumn col : table.getColumns()) {
            header.append(col.getName()).append("\t| ");
        }
        header.append("\t|");
        System.out.println(header);
        IRowIterator rowIt = table.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            StringBuilder rowString = new StringBuilder("| ");
            for (IColumn col : table.getColumns()) {
                rowString.append(col.asString(row)).append("\t| ");
            }
            rowString.append("\t");
            System.out.println(rowString);
            row = rowIt.getNextRow();
        }
    }
}
