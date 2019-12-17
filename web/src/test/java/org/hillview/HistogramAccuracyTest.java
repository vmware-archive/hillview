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

import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.HeatmapSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.results.Heatmap;
import org.hillview.sketches.results.Histogram;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;

import org.hillview.table.columns.DoubleColumnQuantization;

import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.util.List;

@SuppressWarnings("FieldCanBeLocal")
public class HistogramAccuracyTest {
    private static String ontime_directory = "../data/ontime_private/";
    private static String privacy_metadata_name = "privacy_metadata.json";

    @Nullable
    IDataSet<ITable> loadData() {
        try {
            FileSetDescription fsd = new FileSetDescription();
            fsd.fileNamePattern = "../data/ontime_private/????_*.csv*";
            fsd.fileKind = "csv";
            fsd.schemaFile = "short.schema";

            Empty e = Empty.getInstance();
            LocalDataSet<Empty> local = new LocalDataSet<Empty>(e);
            IMap<Empty, List<IFileReference>> finder = new FindFilesMap(fsd);
            IDataSet<IFileReference> found = local.blockingFlatMap(finder);
            IMap<IFileReference, ITable> loader = new LoadFilesMap();
            return found.blockingMap(loader);
        }  catch (Exception ex) {
            // This can happen if the data files have not been generated
            return null;
        }
    }

    void generateHeatmap(int xBuckets, int yBuckets, PrivacySchema ps, IDataSet<ITable> table) {
        String col0 = "DepDelay";
        String col1 = "ArrDelay";
        DoubleColumnQuantization q0 = (DoubleColumnQuantization)ps.quantization(col0);
        DoubleColumnQuantization q1 = (DoubleColumnQuantization)ps.quantization(col1);
        Converters.checkNull(q0);
        Converters.checkNull(q1);
        double epsilon = ps.epsilon(col0, col1);
        DoubleHistogramBuckets b0 = new DoubleHistogramBuckets(q0.globalMin, q0.globalMax, xBuckets);
        DoubleHistogramBuckets b1 = new DoubleHistogramBuckets(q1.globalMin, q1.globalMax, yBuckets);
        IntervalDecomposition d0 = new NumericIntervalDecomposition(q0, b0);
        IntervalDecomposition d1 = new NumericIntervalDecomposition(q1, b1);
        HeatmapSketch sk = new HeatmapSketch(b0, b1, col0, col1, 1.0, 0, q0, q1);
        Heatmap h = table.blockingSketch(sk);
        Assert.assertNotNull(h);
        SecureLaplace laplace = new SecureLaplace(Paths.get("./hillview_test_key"));
        PrivateHeatmap ph = new PrivateHeatmap(d0, d1, h, epsilon, laplace);
        Assert.assertNotNull(ph);
        h = ph.heatmap;
        Assert.assertNotNull(h);
    }

    @Test
    public void coarsenHeatmap() {
        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        PrivacySchema ps = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(ps);
        Assert.assertNotNull(ps.quantization);
        this.generateHeatmap(220, 110, ps, table);
        this.generateHeatmap(110, 55, ps, table);
    }

    @Test
    public void computeAccuracyTest() {
        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        PrivacySchema mdSchema = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);
        ColumnQuantization col1 = mdSchema.quantization.get("DepTime");
        Assert.assertNotNull(col1);
        Assert.assertTrue(col1 instanceof DoubleColumnQuantization);

        DoubleColumnQuantization dq = (DoubleColumnQuantization) col1;

        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        HistogramRequestInfo info = new HistogramRequestInfo(new ColumnDescription("DepTime", ContentsKind.Double),
                0, dq.globalMin, dq.globalMax, dq.getIntervalCount());
        HistogramSketch sk = info.getSketch(col1);
        IntervalDecomposition dd = info.getDecomposition(col1);

        double epsilon = mdSchema.epsilon(info.cd.name);
        System.out.println("Epsilon: " + epsilon);
        Histogram hist = table.blockingSketch(sk); // Leaf counts.
        Assert.assertNotNull(hist);
        SecureLaplace laplace = new SecureLaplace(Paths.get("./hillview_test_key"));
        PrivateHistogram ph = new PrivateHistogram(dd, hist, epsilon, false, laplace);

        int totalLeaves = dd.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        for (int left = 0; left < hist.getBucketCount(); left++) {
            for (int right = left; right < hist.getBucketCount(); right++) {
                ph.noiseForRange(left, right,
                        scale, baseVariance, noise);
                sqtot += Math.pow(noise.noise, 2);
                abstot += Math.abs(noise.noise);
                n++;
            }
        }

        System.out.println("Bucket count: " + hist.getBucketCount());
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);
    }
}
