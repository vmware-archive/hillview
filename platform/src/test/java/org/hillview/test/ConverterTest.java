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

import org.hillview.table.SortedStringsConverterDescription;
import org.hillview.table.api.IStringConverter;
import org.junit.Assert;
import org.junit.Test;

public class ConverterTest extends BaseTest {
    @Test
    public void checkSortedConverter() {
        double eps = .0001;
        String[] data = new String[] {"AA", "AB", "AZ", "B", "Z"};
        SortedStringsConverterDescription ssc = new SortedStringsConverterDescription(data, 0, 10);
        IStringConverter conv = ssc.getConverter();
        Assert.assertTrue(conv.asDouble("A") < 0);
        Assert.assertEquals(conv.asDouble("AA"), 0, eps);
        Assert.assertEquals(conv.asDouble("AB"), 2.5, eps);
        Assert.assertEquals(conv.asDouble("AAB"), 2.5, eps);
        Assert.assertEquals(conv.asDouble("AZ"), 5, eps);
        Assert.assertEquals(conv.asDouble("Z"), 10, eps);
        Assert.assertTrue(conv.asDouble("ZZ") > 10);
    }
}
