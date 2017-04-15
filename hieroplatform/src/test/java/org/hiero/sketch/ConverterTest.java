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

import org.hiero.table.SortedStringsConverter;
import org.junit.Assert;
import org.junit.Test;

public class ConverterTest {
    @Test
    public void checkSortedConverter() {
        double eps = .0001;
        String[] data = new String[] {"AA", "AB", "AZ", "B", "Z"};
        SortedStringsConverter ssc = new SortedStringsConverter(data, 0, 10);
        Assert.assertTrue(ssc.asDouble("A") < 0);
        Assert.assertEquals(ssc.asDouble("AA"), 0, eps);
        Assert.assertEquals(ssc.asDouble("AB"), 2.5, eps);
        Assert.assertEquals(ssc.asDouble("AAB"), 2.5, eps);
        Assert.assertEquals(ssc.asDouble("AZ"), 5, eps);
        Assert.assertEquals(ssc.asDouble("Z"), 10, eps);
        Assert.assertTrue(ssc.asDouble("ZZ") > 10);
    }
}
