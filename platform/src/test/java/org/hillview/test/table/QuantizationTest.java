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

import org.hillview.dataset.LocalDataSet;
import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.results.StringHistogramBuckets;
import org.hillview.table.Table;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.QuantizedColumn;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class QuantizationTest extends BaseTest {
    @Test
    public void testPrivateColumn() {
        Table table = TestTables.testTable();
        IColumn age = table.getLoadedColumn("Age");
        ColumnQuantization cpm = new DoubleColumnQuantization(5, 0, 100);
        QuantizedColumn pc = new QuantizedColumn(age, cpm);
        for (int i = 0; i < pc.sizeInRows(); i++) {
            int v = pc.getInt(i);
            int orig = age.getInt(i);
            Assert.assertEquals(0, v % 5);
            Assert.assertEquals(v, orig - orig % 5);
        }

        IColumn name = table.getLoadedColumn("Name");
        String[] boundaries = new String[26];
        int index = 0;
        for (char c = 'A'; c <= 'Z'; c++) {
            boundaries[index] = Character.toString(c);
            index++;
        }
        cpm = new StringColumnQuantization(boundaries, "a");
        pc = new QuantizedColumn(name, cpm);
        for (int i = 0; i < pc.sizeInRows(); i++) {
            String s = pc.getString(i);
            String orig = name.getString(i);
            Assert.assertNotNull(s);
            Assert.assertNotNull(orig);
            Assert.assertEquals(1, s.length());
            Assert.assertEquals(orig.charAt(0), s.charAt(0));
        }
    }

    @Test
    public void testQuantizedHistogram() {
        Table table = TestTables.testTable();
        IColumn age = table.getLoadedColumn("Age");
        ColumnQuantization cpm = new DoubleColumnQuantization(5, 0, 100);
        QuantizedColumn pcage = new QuantizedColumn(age, cpm);
        IColumn name = table.getLoadedColumn("Name");
        String[] boundaries = new String[26];
        int index = 0;
        for (char c = 'A'; c <= 'Z'; c++) {
            boundaries[index] = Character.toString(c);
            index++;
        }
        cpm = new StringColumnQuantization(boundaries, "a");
        QuantizedColumn pcname = new QuantizedColumn(name, cpm);
        IColumn[] cols = new IColumn[] { pcage, pcname };
        Table quantizedTable = new Table(cols, null, null);
        DoubleHistogramBuckets hb = new DoubleHistogramBuckets("Age", 0, 100, 4);
        HistogramSketch sk = new HistogramSketch(hb, 1.0, 0, null);
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(quantizedTable);
        Histogram histo = local.blockingSketch(sk);
        Assert.assertNotNull(histo);
        Assert.assertEquals(4, histo.getBucketCount());
        long count = 0;
        for (int i = 0; i < histo.getBucketCount(); i++) {
            count += histo.buckets[i];
        }
        Assert.assertEquals(table.getNumOfRows(), count);
    }

    @Test
    public void testQuantizedHistogram1() {
        Table table = TestTables.testTable();
        LocalDataSet<ITable> pub = new LocalDataSet<ITable>(table);

        IColumn age = table.getLoadedColumn("Age");
        ColumnQuantization cpmage = new DoubleColumnQuantization(5, 0, 100);
        QuantizedColumn pcage = new QuantizedColumn(age, cpmage);
        IColumn name = table.getLoadedColumn("Name");
        String[] boundaries = new String[26];
        int index = 0;
        for (char c = 'A'; c <= 'Z'; c++) {
            boundaries[index] = Character.toString(c);
            index++;
        }
        ColumnQuantization cpmname = new StringColumnQuantization(boundaries, "a");
        QuantizedColumn pcname = new QuantizedColumn(name, cpmname);
        IColumn[] cols = new IColumn[] { pcage, pcname };
        Table quantizedTable = new Table(cols, null, null);
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(quantizedTable);

        DoubleHistogramBuckets hb = new DoubleHistogramBuckets("Age", 0, 100, 4);
        HistogramSketch sk = new HistogramSketch(hb, 1.0, 0, null);
        Histogram histo = local.blockingSketch(sk);
        Assert.assertNotNull(histo);
        Assert.assertEquals(4, histo.getBucketCount());
        long count = 0;
        for (int i = 0; i < histo.getBucketCount(); i++) {
            count += histo.buckets[i];
        }
        Assert.assertEquals(table.getNumOfRows(), count);

        HistogramSketch psk = new HistogramSketch(hb, 1.0, 0, cpmage);
        Histogram oh = pub.blockingSketch(psk);
        Assert.assertNotNull(oh);
        Assert.assertEquals(histo.getBucketCount(), oh.getBucketCount());
        for (int i = 0; i < histo.getBucketCount(); i++)
            Assert.assertEquals(histo.buckets[i], oh.buckets[i]);

        StringHistogramBuckets sb = new StringHistogramBuckets("Name", new String[] { "A", "F", "M", "W" });
        HistogramSketch ssk = new HistogramSketch(sb, 1.0, 0, null);
        Histogram shisto = local.blockingSketch(ssk);
        Assert.assertNotNull(shisto);
        Assert.assertEquals(4, shisto.getBucketCount());
        count = 0;
        for (int i = 0; i < histo.getBucketCount(); i++) {
            count += histo.buckets[i];
        }
        Assert.assertEquals(table.getNumOfRows(), count);

        HistogramSketch spsk = new HistogramSketch(sb, 1.0, 0, cpmname);
        Histogram ohs = pub.blockingSketch(spsk);
        Assert.assertNotNull(ohs);
        Assert.assertEquals(shisto.getBucketCount(), ohs.getBucketCount());
        for (int i = 0; i < shisto.getBucketCount(); i++)
            Assert.assertEquals(shisto.buckets[i], ohs.buckets[i]);
    }
}
