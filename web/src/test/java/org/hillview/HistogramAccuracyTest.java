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

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataStructures.DyadicDecomposition;
import org.hillview.dataStructures.HistogramRequestInfo;
import org.hillview.dataStructures.PrivateHistogram;
import org.hillview.dataset.ConcurrentSketch;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.results.Histogram;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;

import org.hillview.table.columns.DoubleColumnQuantization;

import org.hillview.targets.DPWrapper;
import org.hillview.targets.InitialObjectTarget;
import org.hillview.targets.PrivateTableTarget;

import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HistogramAccuracyTest {
    static String ontime_directory = "../data/ontime_private/";
    static String privacy_metadata_name = "privacy_metadata.json";

    @Test
    public void computeAccuracyTest() {
        PrivacySchema mdSchema = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);
        ColumnQuantization col1 = mdSchema.quantization.get("DepTime");
        Assert.assertNotNull(col1);
        Assert.assertTrue(col1 instanceof DoubleColumnQuantization);

        DoubleColumnQuantization dq = (DoubleColumnQuantization)col1;

        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        HistogramRequestInfo info = new HistogramRequestInfo(new ColumnDescription("DepTime", ContentsKind.Double),
                0, dq.globalMin, dq.globalMax, dq.getIntervalCount());
        HistogramSketch sk = info.getSketch(col1);
        DyadicDecomposition dd = info.getDecomposition(col1);

        double epsilon = mdSchema.epsilon(info.cd.name);
        System.out.println("Epsilon: " + epsilon);

        FileSetDescription fsd = new FileSetDescription();
        fsd.fileNamePattern = "../data/ontime_private/????_*.csv*";
        fsd.fileKind = "csv";
        fsd.schemaFile = "short.schema";

        Empty e = new Empty();
        LocalDataSet<Empty> local = new LocalDataSet<Empty>(e);
        String privacyMetadataFile = DPWrapper.privacyMetadataFile(Utilities.getFolder(fsd.fileNamePattern));

        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(fsd);
        IDataSet<IFileReference> found = local.blockingFlatMap(finder);
        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        IDataSet<ITable> table = found.blockingMap(loader);
        DPWrapper wrapper = new DPWrapper(mdSchema);

        Histogram hist = table.blockingSketch(sk); // Leaf counts.
        PrivateHistogram ph = new PrivateHistogram(dd, hist, epsilon, false);

        int totalLeaves = dd.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);
        LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG

        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        for (int left = 0; left < hist.getBucketCount(); left++) {
            for (int right = left; right < hist.getBucketCount(); right++) {
                Pair<Double, Double> noise = dd.noiseForRange(left, right, epsilon,
                        dist, baseVariance, false);
                sqtot += Math.pow(noise.first, 2);
                abstot += Math.abs(noise.first);
                n++;
            }
        }

        System.out.println("Bucket count: " + hist.getBucketCount());
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot/(double)n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot)/(double)n);
    }
}
