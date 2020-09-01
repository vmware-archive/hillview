/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.test.table;

import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.columns.DoubleListColumn;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.hillview.utils.TestTables;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class ConvertColumnTest extends BaseTest {
    static final ContentsKind[] kinds = new ContentsKind[]{
            ContentsKind.None,
            ContentsKind.String,
            ContentsKind.Date,
            ContentsKind.Integer,
            ContentsKind.Json,
            ContentsKind.Double,
            ContentsKind.Interval,
            ContentsKind.Time,
            ContentsKind.Duration
    };

    void convert(IColumn col, ContentsKind[] failing, IMembershipSet set) {
        for (ContentsKind k: kinds) {
            boolean shouldFail = Utilities.contains(failing, k);
            try {
                IColumn result = col.convertKind(k, "x", set);
                Assert.assertNotNull(result);
                Assert.assertFalse(shouldFail);
            } catch (Exception ex) {
                Assert.assertTrue(shouldFail);
            }
        }
    }

    @Test
    public void convert() {
        Table table = TestTables.testTable();
        IColumn age = table.getColumn("Age");
        IMembershipSet set = table.getMembershipSet();
        convert(age, new ContentsKind[] { ContentsKind.None, ContentsKind.Date, ContentsKind.Duration,
                ContentsKind.Interval, ContentsKind.Time }, set);

        IColumn numbers = age.convertKind(ContentsKind.String, "x", set);
        convert(numbers, new ContentsKind[] { ContentsKind.None, ContentsKind.Date, ContentsKind.Duration,
                ContentsKind.Interval, ContentsKind.Time }, set);

        IColumn name = table.getColumn("Name");
        convert(name, new ContentsKind[] { ContentsKind.None, ContentsKind.Date, ContentsKind.Duration,
                ContentsKind.Interval, ContentsKind.Time, ContentsKind.Integer, ContentsKind.Double }, set);

        DoubleListColumn dates = new DoubleListColumn(new ColumnDescription("x", ContentsKind.Date));
        IRowIterator it = set.getIterator();
        int row = it.getNextRow();
        Instant i = Instant.now();
        while (row >= 0) {
            int a = age.getInt(row);
            dates.append(Converters.toDouble(i.minusSeconds(a * 3600 * 24 * 365)));
            row = it.getNextRow();
        }
        /*
        convert(dates, new ContentsKind[] { ContentsKind.None, ContentsKind.Duration,
                ContentsKind.Interval, ContentsKind.Time, ContentsKind.Integer, ContentsKind.Double }, set);
         */
    }
}
