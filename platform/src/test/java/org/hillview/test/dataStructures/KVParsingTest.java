/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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

import org.hillview.utils.KVParsing;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;

/**
 * Tests for the KVParsing class.
 */
public class KVParsingTest {
    @Test
    public void RFC5424StructuredDataTest() {
        KVParsing kvp = KVParsing.createRFC5424StructuredDataParser();
        String data = "[nsx@6876 comp=\"nsx-manager\" subcomp=\"mpa\" tid=\"7074\" level=\"INFO\"]";
        BiFunction<String, String, Boolean> consumer = new BiFunction<String, String, Boolean>() {
            int field = 0;

            @Override
            public Boolean apply(String k, String v) {
                if (field == 0) {
                    Assert.assertEquals("comp", k);
                    Assert.assertEquals("nsx-manager", v);
                } else if (field == 1) {
                    Assert.assertEquals("subcomp", k);
                    Assert.assertEquals("mpa", v);
                } else if (field == 2) {
                    Assert.assertEquals("tid", k);
                    Assert.assertEquals("7074", v);
                } else if (field == 3) {
                    Assert.assertEquals("level", k);
                    Assert.assertEquals("INFO", v);
                } else {
                    throw new RuntimeException("Unexpected " + k + "=" + v);
                }
                field++;
                return false;
            }
        };
        int matches = kvp.parse(data, consumer);
        Assert.assertEquals(4, matches);
    }

    @Test
    public void connectionMatchingTest() {
        String data = "['http_method=\"GET\"', 'http_uri=\".asp\"', 'pcre_http_uri=\"^/.*?\\\\.asp$\"', " +
                "'http_user_agent=\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Win64; x64; Trident/7.0; " + "" +
                ".NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)\"', " +
                "'pcre_http_header=\"Host: \\\\S+\\\\.com\"', 'http_raw_header=\"Accept-Encoding: \"', 'http_raw_header=\"Accept-Language: \"', " +
                "'http_raw_header=\"Connection: \"', 'http_raw_header=\"UA-CPU: \"']";
        KVParsing kvp = KVParsing.createHttpHeaderParser();
        BiFunction<String, String, Boolean> consumer = new BiFunction<String, String, Boolean>() {
            int field = 0;

            @Override
            public Boolean apply(String k, String v) {
                System.out.println(k + "=" + v);
                if (field == 0) {
                    Assert.assertEquals("http_method", k);
                    Assert.assertEquals("GET", v);
                } else if (field == 1) {
                    Assert.assertEquals("http_uri", k);
                    Assert.assertEquals(".asp", v);
                } else if (field == 2) {
                    Assert.assertEquals("pcre_http_uri", k);
                    Assert.assertEquals("^/.*?\\\\.asp$", v);
                } else if (field == 3) {
                    Assert.assertEquals("http_user_agent", k);
                    Assert.assertEquals("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Win64; x64; Trident/7.0; " +
                            ".NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)", v);
                } else if (field == 4) {
                    Assert.assertEquals("pcre_http_header", k);
                    Assert.assertEquals("Host: \\\\S+\\\\.com", v);
                } else if (field == 5) {
                    Assert.assertEquals("http_raw_header", k);
                    Assert.assertEquals("Accept-Encoding: ", v);
                } else if (field == 6) {
                    Assert.assertEquals("http_raw_header", k);
                    Assert.assertEquals("Accept-Language: ", v);
                } else if (field == 7) {
                    Assert.assertEquals("http_raw_header", k);
                    Assert.assertEquals("Connection: ", v);
                } else if (field == 8) {
                    Assert.assertEquals("http_raw_header", k);
                    Assert.assertEquals("UA-CPU: ", v);
                } else {
                    throw new RuntimeException("Unexpected " + k + "=" + v);
                }
                field++;
                return false;
            }
        };
        int matches = kvp.parse(data, consumer);
        Assert.assertEquals(9, matches);
    }
}
