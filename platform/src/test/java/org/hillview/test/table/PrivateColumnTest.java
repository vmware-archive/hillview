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
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.columns.PrivateColumn;
import org.hillview.table.columns.StringColumnPrivacyMetadata;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class PrivateColumnTest extends BaseTest {
    @Test
    public void testPrivateColumn() {
        Table table = TestTables.testTable();
        IColumn age = table.getLoadedColumn("Age");
        ColumnPrivacyMetadata cpm = new DoubleColumnPrivacyMetadata(.1, 5, 0, 100);
        PrivateColumn pc = new PrivateColumn(age, cpm);
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
        cpm = new StringColumnPrivacyMetadata(.1, boundaries, "a");
        pc = new PrivateColumn(name, cpm);
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
    public void testPrivateHistogram() {
        Table table = TestTables.testTable();
        IColumn age = table.getLoadedColumn("Age");
        ColumnPrivacyMetadata cpm = new DoubleColumnPrivacyMetadata(.1, 5, 0, 100);
        PrivateColumn pcage = new PrivateColumn(age, cpm);
        IColumn name = table.getLoadedColumn("Name");
        String[] boundaries = new String[26];
        int index = 0;
        for (char c = 'A'; c <= 'Z'; c++) {
            boundaries[index] = Character.toString(c);
            index++;
        }
        cpm = new StringColumnPrivacyMetadata(.1, boundaries, "a");
        PrivateColumn pcname = new PrivateColumn(name, cpm);
        IColumn[] cols = new IColumn[] { pcage, pcname };
        Table privateTable = new Table(cols, null, null);
        DoubleHistogramBuckets hb = new DoubleHistogramBuckets(0, 100, 4);
        HistogramSketch sk = new HistogramSketch(hb, "Age", 1.0, 0, null);
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(privateTable);
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
    public void testPrivateHistogram1() {
        Table table = TestTables.testTable();
        LocalDataSet<ITable> pub = new LocalDataSet<ITable>(table);

        IColumn age = table.getLoadedColumn("Age");
        ColumnPrivacyMetadata cpmage = new DoubleColumnPrivacyMetadata(.1, 5, 0, 100);
        PrivateColumn pcage = new PrivateColumn(age, cpmage);
        IColumn name = table.getLoadedColumn("Name");
        String[] boundaries = new String[26];
        int index = 0;
        for (char c = 'A'; c <= 'Z'; c++) {
            boundaries[index] = Character.toString(c);
            index++;
        }
        ColumnPrivacyMetadata cpmname = new StringColumnPrivacyMetadata(.1, boundaries, "a");
        PrivateColumn pcname = new PrivateColumn(name, cpmname);
        IColumn[] cols = new IColumn[] { pcage, pcname };
        Table privateTable = new Table(cols, null, null);
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(privateTable);

        DoubleHistogramBuckets hb = new DoubleHistogramBuckets(0, 100, 4);
        HistogramSketch sk = new HistogramSketch(hb, "Age", 1.0, 0, null);
        Histogram histo = local.blockingSketch(sk);
        Assert.assertNotNull(histo);
        Assert.assertEquals(4, histo.getBucketCount());
        long count = 0;
        for (int i = 0; i < histo.getBucketCount(); i++) {
            count += histo.buckets[i];
        }
        Assert.assertEquals(table.getNumOfRows(), count);

        HistogramSketch psk = new HistogramSketch(hb, "Age", 1.0, 0, cpmage);
        Histogram oh = pub.blockingSketch(psk);
        Assert.assertNotNull(oh);
        Assert.assertEquals(histo.getBucketCount(), oh.getBucketCount());
        for (int i = 0; i < histo.getBucketCount(); i++)
            Assert.assertEquals(histo.buckets[i], oh.buckets[i]);

        StringHistogramBuckets sb = new StringHistogramBuckets(new String[] { "A", "F", "M", "W" });
        HistogramSketch ssk = new HistogramSketch(sb, "Name", 1.0, 0, null);
        Histogram shisto = local.blockingSketch(ssk);
        Assert.assertNotNull(shisto);
        Assert.assertEquals(4, shisto.getBucketCount());
        count = 0;
        for (int i = 0; i < histo.getBucketCount(); i++) {
            count += histo.buckets[i];
        }
        Assert.assertEquals(table.getNumOfRows(), count);

        HistogramSketch spsk = new HistogramSketch(sb, "Name", 1.0, 0, cpmname);
        Histogram ohs = pub.blockingSketch(spsk);
        Assert.assertNotNull(ohs);
        Assert.assertEquals(shisto.getBucketCount(), ohs.getBucketCount());
        for (int i = 0; i < shisto.getBucketCount(); i++)
            Assert.assertEquals(shisto.buckets[i], ohs.buckets[i]);
    }
}
