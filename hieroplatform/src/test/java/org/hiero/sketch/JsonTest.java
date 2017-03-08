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

package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.NextKList;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IColumn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JsonTest {
    @Test
    public void convert() {
        ColumnDescription cd0 = new ColumnDescription("Age", ContentsKind.Int, false);
        String s = cd0.toJson();
        assertEquals(s, "{\"name\":\"Age\",\"kind\":\"Int\",\"allowMissing\":false}");

        ColumnDescription cd1 = new ColumnDescription("Weight", ContentsKind.Double, false);
        Schema schema = new Schema();
        schema.append(cd0);
        schema.append(cd1);
        s = schema.toJson();
        assertEquals(s, "[{\"name\":\"Age\",\"kind\":\"Int\",\"allowMissing\":false}," +
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

        List<IColumn> l = new ArrayList<IColumn>();
        l.add(iac);
        l.add(dac);
        l.add(sac);
        SmallTable t = new SmallTable(l);
        // Columns are ordered by name: Age, Name, Weight
        RowSnapshot rs = new RowSnapshot(t, 0);
        s = rs.toJson();
        assertEquals(s, "[10,90.0,\"John\"]");

        s = t.toJson();
        assertEquals(s, "{" +
                "\"schema\":[{\"name\":\"Age\",\"kind\":\"Int\",\"allowMissing\":false}," +
                "{\"name\":\"Weight\",\"kind\":\"Double\",\"allowMissing\":false}," +
                        "{\"name\":\"Name\",\"kind\":\"String\",\"allowMissing\":false}]," +
                "\"rowCount\":2," +
                "\"rows\":[[10,90.0,\"John\"],[20,120.0,\"Mike\"]]" +
        "}");

        List<Integer> li = Arrays.asList(2, 3);
        NextKList list = new NextKList(t, li, 0, 100);
        s = list.toJson();
        assertEquals(s, "{" +
                "\"rowCount\":100," +
                "\"startPosition\":0," +
                "\"rows\":[" +
                    "{\"count\":2,\"values\":[10,90.0,\"John\"]}," +
                    "{\"count\":3,\"values\":[20,120.0,\"Mike\"]}" +
                "]}");
    }
}
