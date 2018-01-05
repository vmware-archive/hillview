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

import org.hillview.table.api.ContentsKind;
import org.hillview.table.rows.GuessSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SchemaTest extends BaseTest {
    @Test
    public void guessSchemaTest() {
        String[] data = new String[] { "1", "3", "0", "1", "2", "1023" };
        GuessSchema gs = new GuessSchema();
        GuessSchema.SchemaInfo info = gs.guess(Arrays.asList(data));
        Assert.assertEquals(ContentsKind.Integer, info.kind);
        Assert.assertEquals(false, info.allowMissing);

        data = new String[] { null, "1", "2", "3.0", "4.5" };
        info = gs.guess(Arrays.asList(data));
        Assert.assertEquals(ContentsKind.Double, info.kind);
        Assert.assertEquals(true, info.allowMissing);

        data = new String[] { "1", "1.0", "Mike"};
        info = gs.guess(Arrays.asList(data));
        Assert.assertEquals(ContentsKind.Category, info.kind);
        Assert.assertEquals(false, info.allowMissing);

        data = new String[] { "1", "1.0", "\"string\"", "{}", "[0, 1, 2]", "{ \"a\": 1 }"};
        info = gs.guess(Arrays.asList(data));
        Assert.assertEquals(ContentsKind.Json, info.kind);
        Assert.assertEquals(false, info.allowMissing);

        data = new String[] { "2010/10/25 10:20", "2013/03/08 11:26", "2012/1/1 5:30"};
        info = gs.guess(Arrays.asList(data));
        Assert.assertEquals(ContentsKind.Date, info.kind);
        Assert.assertEquals(false, info.allowMissing);
    }
}
