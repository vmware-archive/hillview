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

package org.hillview.test;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.IntArrayColumn;
import org.junit.Test;

import java.util.function.Consumer;

public class ColumnPerfTest {
    @Test
    public void testColumnGetInt() {
        final IntArrayColumn col;
        final int size = 1000000;
        final int testNum = 10;
        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Integer, false);

        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);
        final Consumer<Integer>  fCall = tmp -> {
            for (int i = 0; i < size; i++)
                tmp += col.getInt(i);
        };
        TestUtil.runPerfTest(fCall,testNum);
        final int[] rCol = new int[size];
        for (int i=0; i < size; i++)
            rCol[i]=i;
        final Consumer<Integer> dCall = tmp -> {
            for (int i = 0; i < size; i++)
                tmp += rCol[i];
        };
        TestUtil.runPerfTest(dCall, testNum);
    }
}
