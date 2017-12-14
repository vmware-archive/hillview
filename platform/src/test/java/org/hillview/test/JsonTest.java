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

import org.hillview.dataset.api.IJson;
import org.hillview.sketches.NextKList;
import org.hillview.storage.JsonFileLoader;
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.table.rows.RowSnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JsonTest extends BaseTest {
    @Test
    public void convert() {
        ColumnDescription cd0 = new ColumnDescription("Age", ContentsKind.Integer, false);
        String s = cd0.toJson();
        Assert.assertEquals(s, "{\"name\":\"Age\",\"kind\":\"Integer\",\"allowMissing\":false}");

        ColumnDescription cd1 = new ColumnDescription("Weight", ContentsKind.Double, false);
        Schema schema = new Schema();
        schema.append(cd0);
        schema.append(cd1);
        s = schema.toJson();
        Assert.assertEquals(s, "[{\"name\":\"Age\",\"kind\":\"Integer\",\"allowMissing\":false}," +
        "{\"name\":\"Weight\",\"kind\":\"Double\",\"allowMissing\":false}]");

        ColumnDescription cd2 = new ColumnDescription("Name", ContentsKind.String, false);

        IntArrayColumn iac = new IntArrayColumn(cd0, 2);
        iac.set(0, 10);
        iac.set(1, 20);

        DoubleArrayColumn dac = new DoubleArrayColumn(cd1, 2);
        dac.set(0, 90.0);
        dac.set(1, 120.0);

        StringArrayColumn sac = new StringArrayColumn(cd2, 2);
        sac.set(0, "John");
        sac.set(1, "Mike");

        IColumn[] l = new IColumn[3];
        l[0] = iac;
        l[1] = dac;
        l[2] = sac;
        SmallTable t = new SmallTable(l);
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        RowSnapshot rs = new RowSnapshot(t, 0);
        s = rs.toJson();
        Assert.assertEquals(s, "[10,90.0,\"John\"]");

        s = t.toJson();
        Assert.assertEquals(s, "{" +
                "\"schema\":[{\"name\":\"Age\",\"kind\":\"Integer\",\"allowMissing\":false}," +
                "{\"name\":\"Weight\",\"kind\":\"Double\",\"allowMissing\":false}," +
                        "{\"name\":\"Name\",\"kind\":\"String\",\"allowMissing\":false}]," +
                "\"rowCount\":2," +
                "\"rows\":[[10,90.0,\"John\"],[20,120.0,\"Mike\"]]" +
        "}");

        Schema back = IJson.gsonInstance.fromJson(schema.toJson(), Schema.class);
        Assert.assertEquals(schema, back);

        List<Integer> li = Arrays.asList(2, 3);
        NextKList list = new NextKList(t, li, 0, 100);
        s = list.toJson();
        Assert.assertEquals(s, "{" +
                "\"rowCount\":100," +
                "\"startPosition\":0," +
                "\"rows\":[" +
                    "{\"count\":2,\"values\":[10,90.0,\"John\"]}," +
                    "{\"count\":3,\"values\":[20,120.0,\"Mike\"]}" +
                "]}");
    }

    @Test
    public void jsonReaderTest() {
        final String jsonFolder = "../data/ontime";
        final String jsonSample = "short.schema";
        JsonFileLoader reader = new JsonFileLoader(jsonFolder + "/" + jsonSample, null);
        ITable table = reader.load();
        Assert.assertEquals("Table[3x15]", table.toString());
    }
}
