/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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

import org.hiero.sketch.table.api.IColumn;

import java.util.ArrayList;

class RowsAsStrings {
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
