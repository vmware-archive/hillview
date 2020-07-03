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

package org.hillview.benchmarks;

import com.google.gson.*;
import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.TableSketch;
import org.hillview.sketches.Histogram2DSketch;
import org.hillview.sketches.results.*;
import org.hillview.targets.TableRpcTarget;
import org.hillview.main.Benchmarks;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.security.SecureLaplace;
import org.hillview.security.TestKeyLoader;
import org.hillview.sketches.SummarySketch;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;

import org.hillview.table.columns.DoubleColumnQuantization;

import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.*;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

public class DPAccuracyBenchmarks extends Benchmarks {
    private final static String ontime_directory = "../data/ontime_private/";
    private final static String privacy_metadata_name = "privacy_metadata.json";
    private static final String histogram_results_filename = "../results/ontime_private_histogram.json";
    private static final String heatmap_results_filename = "../results/ontime_private_heatmap.json";

    final String resultsFilename;

    private DPAccuracyBenchmarks(String resultsFilename) {
        this.resultsFilename = resultsFilename;
    }

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

    Schema loadSchema(IDataSet<ITable> data) {
        SummarySketch sk = new SummarySketch();
        TableSummary tableSummary = data.blockingSketch(sk);
        assert tableSummary != null;
        if (tableSummary.rowCount == 0)
            throw new RuntimeException("No file data loaded");
        return Converters.checkNull(tableSummary.schema);
    }

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private histogram
     * averaged over all possible range queries.
     *
     * @param ph The histogram whose accuracy we want to compute.
     * @param decomposition The leaf specification for the histogram.
     * @return The average per-query absolute error.
     */
    private double computeAccuracy(DPHistogram<IGroup<Count>> ph, IntervalDecomposition decomposition, SecureLaplace laplace) {
        double scale = PrivacyUtils.computeNoiseScale(ph.getEpsilon(), decomposition);
        double baseVariance = PrivacyUtils.laplaceVariance(scale);
        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        int bucketCount = decomposition.getBucketCount();
        for (int left = 0; left < bucketCount; left++) {
            for (int right = left; right < bucketCount; right++) {
                ph.noiseForRange(left, right, scale, baseVariance, laplace, noise);
                sqtot += Math.pow(noise.getNoise(), 2);
                abstot += Math.abs(noise.getNoise());
                n++;
            }
        }

        System.out.println("Bucket count: " + bucketCount);
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);

        return abstot / (double) n;
    }

    /**
     * Compute the absolute and L2 error vs. ground truth for an instantiation of a private heat map
     * averaged over all possible range queries (every rectangle).
     *
     * @param ph The heat map whose accuracy we want to compute.
     * @param xd The leaf specification for the x-axis.
     * @param yd The leaf specification for the y-axis.
     * @return The average per-query absolute error.
     */
    private Double computeAccuracy(DPHeatmapSketch<IGroup<Count>, IGroup<IGroup<Count>>> ph,
                                   IntervalDecomposition xd, IntervalDecomposition yd) {
        double scale = PrivacyUtils.computeNoiseScale(ph.getEpsilon(), xd, yd);
        double baseVariance = PrivacyUtils.laplaceVariance(scale);

        // Do all-intervals accuracy on leaves.
        int n = 0;
        double sqtot = 0.0;
        double abstot = 0.0;
        Noise noise = new Noise();
        int xBucketCount = xd.getBucketCount();
        int yBucketCount = yd.getBucketCount();
        for (int left = 0; left < xBucketCount; left++) {
            for (int right = left; right < xBucketCount; right++) {
                for (int top = 0; top < yBucketCount; top++) {
                    for (int bot = top; bot < yBucketCount; bot++) {
                        ph.noiseForRange(left, right, top, bot,
                                scale, baseVariance, noise);
                        sqtot += Math.pow(noise.getNoise(), 2);
                        abstot += Math.abs(noise.getNoise());
                        n++;
                    }
                }
            }
        }

        System.out.println("Bucket count: " + xBucketCount * yBucketCount);
        System.out.println("Num intervals: " + n);
        System.out.println("Average absolute error: " + abstot / (double) n);
        System.out.println("Average L2 error: " + Math.sqrt(sqtot) / (double) n);

        return abstot / (double) n;
    }

    private TableRpcTarget.HistogramRequestInfo createHistogramRequest(String col, ColumnQuantization cq) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        TableRpcTarget.HistogramRequestInfo info;
        if (cq instanceof DoubleColumnQuantization) {
            DoubleColumnQuantization dq = (DoubleColumnQuantization)cq;
            info = new TableRpcTarget.HistogramRequestInfo(new ColumnDescription(col, ContentsKind.Double),
                    0, dq.globalMin, dq.globalMax, dq.getIntervalCount());
        } else {
            // StringColumnQuantization
            StringColumnQuantization sq = (StringColumnQuantization)cq;
            info = new TableRpcTarget.HistogramRequestInfo(new ColumnDescription(col, ContentsKind.String),0, sq.leftBoundaries);
        }

        return info;
    }

    private Pair<Double, Double> computeSingleColumnAccuracy(
            String col, int colIndex,
            ColumnQuantization cq, double epsilon, IDataSet<ITable> table,
            int iterations) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        TableRpcTarget.HistogramRequestInfo info = createHistogramRequest(col, cq);
        TableSketch<Groups<Count>> sk = info.getSketch(cq);
        IntervalDecomposition dd = info.getDecomposition(cq);

        System.out.println("Epsilon: " + epsilon);
        Groups<Count> hist = table.blockingSketch(sk); // Leaf counts.
        Assert.assertNotNull(hist);

        TestKeyLoader tkl = new TestKeyLoader();
        ArrayList<Double> accuracies = new ArrayList<>();
        double totAccuracy = 0.0;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            @SuppressWarnings("ConstantConditions")
            DPHistogram<IGroup<Count>> ph = new DPHistogram<>(null, colIndex, dd, epsilon, false, laplace);
            double acc = computeAccuracy(ph, dd, laplace);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair<Double, Double>(totAccuracy / iterations, Utilities.stdev(accuracies));
    }

    private Pair<Double, Double> computeHeatmapAccuracy(String col1, ColumnQuantization cq1,
                                                        String col2, ColumnQuantization cq2,
                                                        int columnsIndex,
                                                        double epsilon, IDataSet<ITable> table,
                                                        int iterations) {
        // Construct a histogram corresponding to the leaves.
        // We will manually aggregate buckets as needed for the accuracy test.
        TableRpcTarget.HistogramRequestInfo[] info = new TableRpcTarget.HistogramRequestInfo[]
                {
                        createHistogramRequest(col1, cq1),
                        createHistogramRequest(col2, cq2)
                };

        Histogram2DSketch sk = new Histogram2DSketch(
                info[0].getBuckets(), info[1].getBuckets());
        IntervalDecomposition d0 = info[0].getDecomposition(cq1);
        IntervalDecomposition d1 = info[1].getDecomposition(cq2);

        System.out.println("Epsilon: " + epsilon);
        Groups<Groups<Count>> heatmap = table.blockingSketch(sk); // Leaf counts.
        Assert.assertNotNull(heatmap);

        TestKeyLoader tkl = new TestKeyLoader();
        ArrayList<Double> accuracies = new ArrayList<>();
        double totAccuracy = 0.0;
        for (int i = 0 ; i < iterations; i++) {
            tkl.setIndex(i);
            SecureLaplace laplace = new SecureLaplace(tkl);
            @SuppressWarnings("ConstantConditions")
            DPHeatmapSketch<IGroup<Count>, IGroup<IGroup<Count>>> ph =
                    new DPHeatmapSketch<>(null, columnsIndex, d0, d1, epsilon, laplace);
            double acc = computeAccuracy(ph, d0, d1);
            accuracies.add(acc);
            totAccuracy += acc;
        }
        return new Pair<Double, Double>(totAccuracy / iterations, Utilities.stdev(accuracies));
    }

    public void benchmarkHistogramL2Accuracy() throws IOException {
        HillviewLogger.instance.setLogLevel(Level.OFF);
        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        Schema schema = this.loadSchema(table);
        List<String> cols = schema.getColumnNames();

        PrivacySchema mdSchema = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int iterations = 5;
        for (String col : cols) {
            ColumnQuantization quantization = mdSchema.quantization.get(col);
            Assert.assertNotNull(quantization);

            double epsilon = mdSchema.epsilon(col);

            Pair<Double, Double> res = this.computeSingleColumnAccuracy(col, mdSchema.getColumnIndex(col), quantization, epsilon, table, iterations);
            System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

            // for JSON parsing convenience
            ArrayList<Double> resArr = new ArrayList<Double>();
            resArr.add(res.first); // noise
            resArr.add(res.second); // stdev

            results.put(col, resArr);
        }

        FileWriter writer = new FileWriter(histogram_results_filename);
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }

    public void benchmarkHeatmapL2Accuracy() throws IOException {
        HillviewLogger.instance.setLogLevel(Level.OFF);
        @Nullable
        IDataSet<ITable> table = this.loadData();
        if (table == null) {
            System.out.println("Skipping test: no data");
            return;
        }

        Schema schema = this.loadSchema(table);
        List<String> cols = schema.getColumnNames();

        PrivacySchema mdSchema = PrivacySchema.loadFromFile(ontime_directory + privacy_metadata_name);
        Assert.assertNotNull(mdSchema);
        Assert.assertNotNull(mdSchema.quantization);

        HashMap<String, ArrayList<Double>> results = new HashMap<String, ArrayList<Double>>();
        int iterations = 5;
        for (String col1 : cols) {
            for (String col2 : cols) {
                if (col1.equals(col2)) continue;

                ColumnQuantization q1 = mdSchema.quantization.get(col1);
                Assert.assertNotNull(q1);
                ColumnQuantization q2 = mdSchema.quantization.get(col2);
                Assert.assertNotNull(q2);

                String key = mdSchema.getKeyForColumns(col1, col2);
                double epsilon = mdSchema.epsilon(key);

                Pair<Double, Double> res = this.computeHeatmapAccuracy(col1, q1, col2, q2, mdSchema.getColumnIndex(col1, col2),
                        epsilon, table, iterations);
                System.out.println("Averaged absolute error over " + iterations + " iterations: " + res.first);

                // for JSON parsing convenience
                ArrayList<Double> resArr = new ArrayList<Double>();
                resArr.add(res.first); // noise
                resArr.add(res.second); // stdev

                System.out.println("Key: " + key + ", mean: " + res.first);
                results.put(key, resArr);
            }
        }

        FileWriter writer = new FileWriter(heatmap_results_filename);
        Gson resultsGson = new GsonBuilder().create();
        writer.write(resultsGson.toJson(results));
        writer.flush();
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            return;
        }

        String resultsFilename = args[0];
        DPAccuracyBenchmarks bench = new DPAccuracyBenchmarks(resultsFilename);
        bench.benchmarkHeatmapL2Accuracy();
    }
}
