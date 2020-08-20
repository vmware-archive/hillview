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

import org.hillview.utils.DateParsing;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Randomness;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.logging.Level;

/**
 * Base test class, used for global setup and teardown.
 * If your test is not thread-safe, use the following annotation
 * on the test class:
 * "@net.jcip.annotations.NotThreadSafe"
 */
@SuppressWarnings("WeakerAccess")
@Ignore
public class BaseTest {
    private static boolean initialized = false;
    protected static final boolean toPrint = false;
    protected static final String dataDir = "../data";

    public static LocalDateTime parseOneLocalDate(String s) {
        DateParsing parser = new DateParsing(s);
        return parser.parseLocalDate(s);
    }

    @BeforeClass
    public static void setup() {
        if (!initialized) {
            HillviewLogger.initialize("test", null);
            HillviewLogger.instance.setLogLevel(Level.WARNING);
        }
        initialized = true;
    }

    /**
     * Called when an exception should be ignored.
     */
    public void ignoringException(String message, Throwable t) {
         System.out.println(message);
         t.printStackTrace();
    }

    /**
     * Randomness is actually not thread-safe!
     */
    public Randomness getRandomness() {
        return new Randomness(0);
    }
}
