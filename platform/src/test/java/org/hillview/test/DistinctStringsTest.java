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

import org.hillview.sketches.DistinctStrings;
import org.junit.Assert;
import org.junit.Test;

public class DistinctStringsTest {
    @Test
    public void distinctTest() {
        DistinctStrings ds = new DistinctStrings(5);
        ds.add(null);
        ds.add("1");
        ds.add("2");
        Assert.assertEquals(ds.size(), 3);
        ds.add("4");
        ds.add("5");
        ds.add("6");
        Assert.assertEquals(ds.size(), 5);
        Assert.assertEquals(ds.truncated, true);
    }
}
