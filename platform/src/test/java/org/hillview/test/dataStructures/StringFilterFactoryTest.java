/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.test.dataStructures;

import org.hillview.table.api.IStringFilter;
import org.hillview.table.filters.StringFilterFactory;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class StringFilterFactoryTest extends BaseTest {
    @Test
    public void testStringFilter() {
        StringFilterDescription desc = new StringFilterDescription("bob", true, false, false, false, false, false);
        IStringFilter filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertTrue(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));

        desc = new StringFilterDescription("bob", false, false, true, false, false, false);
        filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertFalse(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));

        desc = new StringFilterDescription("^bo.*", false, true, false, false, false, false);
        filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertTrue(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));

        desc = new StringFilterDescription("bo.*", false, true, true, false, false, false);
        filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertFalse(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));

        desc = new StringFilterDescription("bo", true, false, true, false, false, false);
        filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertFalse(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));

        desc = new StringFilterDescription("bo", true, false, false, false, false, false);
        filter = StringFilterFactory.getFilter(desc);
        Assert.assertTrue(filter.test("bob"));
        Assert.assertTrue(filter.test("Bob"));
        Assert.assertFalse(filter.test("mike"));
    }
}
