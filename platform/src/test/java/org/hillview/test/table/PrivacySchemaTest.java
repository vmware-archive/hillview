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

package org.hillview.test.table;

import org.hillview.dataset.api.IJson;
import org.hillview.table.PrivacySchema;
import org.hillview.table.QuantizationSchema;
import org.hillview.table.columns.ColumnQuantization;

import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import javax.management.loading.PrivateClassLoader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class PrivacySchemaTest extends BaseTest {
    @Test
    public void parseMetadataTest() {
        String metadata = "{\"quantization\":{\"quantization\":{\"col1\":{\"type\":\"DoubleColumnQuantization\",\"granularity\":12.345,\"globalMin\":0.0,\"globalMax\":123.45},\"col2\":{\"type\":\"StringColumnQuantization\",\"globalMax\":\"d\",\"leftBoundaries\":[\"a\",\"b\",\"c\"]}}},\"epsilons\":{\"col1\":0.1,\"col2\":0.5},\"defaultEpsilons\":{},\"defaultEpsilon\":0.001}";
        PrivacySchema mdSchema = PrivacySchema.loadFromString(metadata);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);
        ColumnQuantization col1 = mdSchema.quantization.get("col1");
        Assert.assertNotNull(col1);
        Assert.assertTrue(col1 instanceof DoubleColumnQuantization);
        ColumnQuantization col2 = mdSchema.quantization.get("col2");
        Assert.assertNotNull(col2);
        Assert.assertTrue(col2 instanceof StringColumnQuantization);
        double eps = mdSchema.epsilon("col1");
        Assert.assertEquals(0.1, eps, 0.001);
        eps = mdSchema.epsilon("not there");
        Assert.assertEquals(.001, eps, .0001);
    }

    @Test
    public void serializeMetadataTest() {
        HashMap<String, ColumnQuantization> mdMap = new HashMap<String, ColumnQuantization>();
        ColumnQuantization md1 = new DoubleColumnQuantization(12.345, 0.0, 123.45);
        ColumnQuantization md2 = new StringColumnQuantization(new String[] {"a", "b", "c"}, "d");
        QuantizationSchema qs = new QuantizationSchema();
        qs.set("col1", md1);
        qs.set("col2", md2);
        PrivacySchema mdSchema = new PrivacySchema(qs);
        mdSchema.setEpsilon("col1", .1);
        mdSchema.setEpsilon("col2", .5);
        String mdJson = mdSchema.toJson();
        String expected = "{\"quantization\":{\"quantization\":{\"col1\":{\"type\":\"DoubleColumnQuantization\",\"granularity\":12.345,\"globalMin\":0.0,\"globalMax\":123.45},\"col2\":{\"type\":\"StringColumnQuantization\",\"globalMax\":\"d\",\"leftBoundaries\":[\"a\",\"b\",\"c\"]}}},\"epsilons\":{\"col1\":0.1,\"col2\":0.5},\"defaultEpsilons\":{},\"defaultEpsilon\":0.001}";
        Assert.assertEquals(expected, mdJson);
    }

    @Test
    public void serializeMultipleColumnsTest() {
        HashMap<String, ColumnQuantization> mdMap = new HashMap<String, ColumnQuantization>();
        ColumnQuantization md1 = new DoubleColumnQuantization(12.345, 0.0, 123.45);
        ColumnQuantization md2 = new DoubleColumnQuantization(0.5, -0.5, 13.0);
        QuantizationSchema qs = new QuantizationSchema();
        qs.set("col1", md1);
        qs.set("col2", md2);
        PrivacySchema msSchema = new PrivacySchema(qs);
        msSchema.setEpsilon("col1+col2", .25);
        msSchema.setEpsilon("col1", .1);
        msSchema.setEpsilon("col1", .5);
        String mdJson = msSchema.toJson();
        String expected = "{\"quantization\":{\"quantization\":{\"col1\":{\"type\":\"DoubleColumnQuantization\",\"granularity\":12.345,\"globalMin\":0.0,\"globalMax\":123.45},\"col2\":{\"type\":\"DoubleColumnQuantization\",\"granularity\":0.5,\"globalMin\":-0.5,\"globalMax\":13.0}}},\"epsilons\":{\"col1+col2\":0.25,\"col1\":0.5},\"defaultEpsilons\":{},\"defaultEpsilon\":0.001}";
        Assert.assertEquals(expected, mdJson);
    }

    @Test
    public void deserializeMultipleColumnsTest() {
        String md = "{'quantization':{'quantization':{'col1':{'type':'DoubleColumnQuantization','granularity':12.345,'globalMin':0.0,'globalMax':123.45},'col2':{'type':'DoubleColumnQuantization','granularity':0.5,'globalMin':-0.5,'globalMax':13.0}}},'epsilons':{'col1+col2':0.25,'col1':0.5},\"defaultEpsilons\":{\"1\":1,\"2\":1},\"defaultEpsilon\":0.001}";
        PrivacySchema mdSchema = PrivacySchema.loadFromString(md);
        double eps = mdSchema.epsilon("col1", "col2");
        Assert.assertEquals(.25, eps, 0.001);
        eps = mdSchema.epsilon("not there");
        Assert.assertEquals(1, eps, .0001);
        eps = mdSchema.epsilon("col1", "col2", "col3");
        Assert.assertEquals(.001, eps, .0001);
    }

    @Test
    public void saveTest() throws IOException {
        HashMap<String, ColumnQuantization> mdMap = new HashMap<String, ColumnQuantization>();
        ColumnQuantization md1 = new DoubleColumnQuantization(12.345, 0.0, 123.45);
        ColumnQuantization md2 = new StringColumnQuantization(new String[] {"a", "b", "c"}, "d");
        QuantizationSchema qs = new QuantizationSchema();
        qs.set("col1", md1);
        qs.set("col2", md2);
        PrivacySchema mdSchema = new PrivacySchema(qs);
        mdSchema.setEpsilon("col1", .1);
        mdSchema.setEpsilon("col2", .5);

        String fname = "test.json";
        mdSchema.saveToFile(fname);
        assert(Files.exists(Paths.get(fname)));

        PrivacySchema loadSchema = PrivacySchema.loadFromFile(fname);
        assert(IJson.gsonInstance.toJson(loadSchema).equals(IJson.gsonInstance.toJson(mdSchema)));

        Files.delete(Paths.get(fname));
    }
}
