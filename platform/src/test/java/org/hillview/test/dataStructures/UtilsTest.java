/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import org.hillview.test.BaseTest;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class UtilsTest extends BaseTest {
    @Test
    public void testWildcard() {
        String s = Utilities.wildcardToRegex("simple");
        Assert.assertEquals("^simple$", s);
        s = Utilities.wildcardToRegex("*");
        Assert.assertEquals("^.*$", s);
        s = Utilities.wildcardToRegex("file.*");
        Assert.assertEquals("^file\\..*$", s);
        s = Utilities.wildcardToRegex("*.file.*");
        Assert.assertEquals("^.*\\.file\\..*$", s);
    }

    @Test
    public void testTrim() {
        String s = Utilities.trim("Some string", ' ');
        Assert.assertEquals("Some string", s);
        s = Utilities.trim(" Some string", ' ');
        Assert.assertEquals("Some string", s);
        s = Utilities.trim("  Some string ", ' ');
        Assert.assertEquals("Some string", s);
        s = Utilities.trim("\"Quotes\"", '"');
        Assert.assertEquals("Quotes", s);
    }

    @Test
    public void testGetKV() {
        @Nullable String s;

        //noinspection ConstantConditions
        s = Utilities.getKV(null, "key");
        //noinspection ConstantConditions
        Assert.assertNull(s);

        s = Utilities.getKV("", "key");
        Assert.assertNull(s);

        s = Utilities.getKV("some string", "key");
        Assert.assertNull(s);

        s = Utilities.getKV("prefix key=value", "key");
        Assert.assertEquals("value", s);

        s = Utilities.getKV("prefix key=\"value\"", "key");
        Assert.assertEquals("value", s);
    }

    @Test
    public void testSingleSpaced() {
        String s = Utilities.singleSpaced("NOSPACES");
        Assert.assertEquals("NOSPACES", s);

        s = Utilities.singleSpaced("ONE SPACE");
        Assert.assertEquals("ONE SPACE", s);

        s = Utilities.singleSpaced("TWO  SPACES");
        Assert.assertEquals("TWO SPACES", s);
    }
}
