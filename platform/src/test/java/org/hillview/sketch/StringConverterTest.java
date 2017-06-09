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

package org.hillview.sketch;

import org.hillview.table.ExplicitStringConverter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringConverterTest {
    private final ExplicitStringConverter converter;

    public StringConverterTest() {
        this.converter = new ExplicitStringConverter();
        this.converter.set("S", 0);
    }

    @Test(expected=NullPointerException.class)
    public void getNonExistent() {
        this.converter.asDouble("T");
    }

    @Test
    public void testMember() {
        assertEquals( this.converter.asDouble("S"), 0.0, 1e-3 );
    }
}
