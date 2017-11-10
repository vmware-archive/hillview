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

package org.hillview.test;

import org.hillview.storage.ParquetReader;
import org.hillview.table.api.ITable;
import org.junit.Test;

public class ParquetTest extends BaseTest {
    @Test
    public void readTest() {
        try {
            // This is an Impala-produced file which is not yet checked-in into the repository
            String path = "../data/parquet/" +
            "part-r-00000-9d5cd245-a2e4-4002-9d58-0efdfb0fb962.gz.parquet";
            ParquetReader pr = new ParquetReader(path);
            ITable table = pr.read();
        } catch (Exception ex) {
            // If the file is not present do not fail the test.
        }
    }
}
