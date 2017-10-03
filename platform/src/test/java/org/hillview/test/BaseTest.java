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

import org.hillview.utils.HillviewLogger;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Base test class, used for global setup and teardown.
 */
@Ignore
public class BaseTest {
    @BeforeClass
    public static void setup() {
        //noinspection ConstantConditions
        if (HillviewLogger.instance == null)
            HillviewLogger.initialize("test.log");
    }
}
