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

package org.hillview;

import org.hillview.dataStructures.PrivacySchema;
import org.hillview.table.columns.ColumnPrivacyMetadata;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class PrivacySchemaTest {
    @Test
    public void parseMetadataTest() {
        String metadata = "{'metadata':{" +
                "'col1':{" +
                "'epsilon':0.1," +
                "'granularity':10.5," +
                "'globalMin':0.0," +
                "'globalMax':123.45}," +
                "'col2':{" +
                "'epsilon':0.5," +
                "'granularity':15.0," +
                "'globalMin':-0.5," +
                "'globalMax':13.1}" +
                "}}";
        PrivacySchema mdSchema = PrivacySchema.loadFromString(metadata);
        assertEquals(mdSchema.get("col1").epsilon, 0.1, 0.001);
    }

    @Test
    public void serializeMetadataTest() {
        HashMap<String, ColumnPrivacyMetadata> mdMap = new HashMap<String, ColumnPrivacyMetadata>();
        ColumnPrivacyMetadata md1 = new ColumnPrivacyMetadata(0.1, 12.345, 0.0, 123.45);
        ColumnPrivacyMetadata md2 = new ColumnPrivacyMetadata(0.5, 0.5, -0.5, 13.0);
        mdMap.put("col1", md1);
        mdMap.put("col2", md2);
        PrivacySchema mdSchema = new PrivacySchema(mdMap);
        String mdJson = mdSchema.toJson();
        String expected = "{\"metadata\":{\"col2\":{\"epsilon\":0.5,\"granularity\":0.5,\"globalMin\":-0.5,\"globalMax\":13.0}" +
                ",\"col1\":{\"epsilon\":0.1,\"granularity\":12.345,\"globalMin\":0.0,\"globalMax\":123.45}}}";
        assertEquals(expected, mdJson);
    }
}
