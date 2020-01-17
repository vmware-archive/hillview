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

import org.hillview.dataStructures.*;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.main.Benchmarks;
import org.hillview.management.ClusterConfig;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.HeatmapSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.results.*;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.PrivacySchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.targets.DPWrapper;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.HostList;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * This is a main class for running various differential privacy benchmarks.
 * The measurements are run headless.
 */
public class DPPerfBenchmarks extends Benchmarks {
    private static int runCount = 7;
    @SuppressWarnings("FieldCanBeLocal")
    private static int buckets = 100;
    @Nullable
    private Schema ontimeSchema;

    @Nullable
    private IDataSet<ITable> flights;
    @Nullable
    private HashMap<Integer, IDataSet<ITable>> cloudFlights; // maps machine count to dataset
    private DPWrapper flightsWrapper;
    private JdbcDatabase database;
    private static boolean quiet = true;

    private static long time(Runnable runnable) {
        long start = System.nanoTime();
        runnable.run();
        long end = System.nanoTime();
        return end - start;
    }

    protected static void runNTimes(Runnable runnable, int count, String message) {
        for (int i=0; i < count; i++) {
            long t = time(runnable);
            if (!quiet)
                System.out.println(message + "," + i + "," + (t/(1000.0 * 1000.0)));
        }
    }

    private DPPerfBenchmarks(HashSet<String> datasets) throws SQLException, IOException {
        this.loadData(datasets);
    }

    private void loadData(HashSet<String> datasets) throws SQLException, IOException {
        this.ontimeSchema = Schema.readFromJsonFile(Paths.get("data/ontime/short.schema"));
        String privacyMetadataFile = DPWrapper.privacyMetadataFile("data/ontime_private");
        assert privacyMetadataFile != null;
        PrivacySchema ps = PrivacySchema.loadFromFile(privacyMetadataFile);
        this.flightsWrapper = new DPWrapper(ps, privacyMetadataFile);

        if (datasets.contains("DB")) {
            System.err.println("Loading database");
            JdbcConnectionInformation conn = new JdbcConnectionInformation();
            conn.host = "localhost";
            conn.database = "flights";
            conn.table = "flights";
            conn.user = "user";
            conn.password = "password";
            conn.databaseKind = "mysql";
            conn.port = 3306;
            conn.lazyLoading = false;
            this.database = new JdbcDatabase(conn);
            this.database.connect();
        }

        if (datasets.contains("Local")) {
            FileSetDescription desc = new FileSetDescription();
            desc.fileKind = "csv";
            desc.headerRow = true;
            desc.fileNamePattern = "data/bench/2017*.csv*";
            desc.schemaFile = "short.schema";
            System.err.println("Loading dataset");
            LocalDataSet<Empty> start = new LocalDataSet<Empty>(Empty.getInstance());
            this.flights = this.loadTable(start, desc);
            IDataSet<ITable> data = this.flights;
            SummarySketch sk = new SummarySketch();
            TableSummary tableSummary = data.blockingSketch(sk);
            assert tableSummary != null;
            if (tableSummary.rowCount == 0)
                throw new RuntimeException("No file data loaded");
        }

        if (datasets.contains("Cloud")) {
            System.out.println("Loading cloud dataset");
            // Load the flights dataset in the cloud.
            ClusterConfig config = ClusterConfig.parse("config-aws.json");

            HostList workers = config.getWorkers();
            List<Integer> workerCounts = new ArrayList<Integer>();
            for (int i = 1; i < workers.size(); i *= 2)
                workerCounts.add(i);
            if (!workerCounts.contains(workers.size()))
                workerCounts.add(workers.size());
            this.cloudFlights = new HashMap<Integer, IDataSet<ITable>>();
            FileSetDescription desc = new FileSetDescription();
            desc.fileKind = "orc";
            desc.fileNamePattern = "data/ontime_small_orc/*.orc";
            desc.schemaFile = "schema";

            for (int i: workerCounts) {
                HostList subset = new HostList(workers.getServerList().subList(0, i));
                IDataSet<Empty> clusterInit = RemoteDataSet.createCluster(subset, RemoteDataSet.defaultDatasetIndex);
                IDataSet<ITable> table = this.loadTable(clusterInit, desc);
                this.cloudFlights.put(i, table);
                SummarySketch sk = new SummarySketch();
                TableSummary tableSummary = table.blockingSketch(sk);
                assert tableSummary != null;
                if (tableSummary.rowCount == 0)
                    throw new RuntimeException("No file data loaded");
            }
        }
    }

    private IDataSet<ITable> loadTable(IDataSet<Empty> start, FileSetDescription desc) {
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(desc);
        IDataSet<IFileReference> found = start.blockingFlatMap(finder);
        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        return found.blockingMap(loader);
    }

    enum Dataset {
        Local,
        DB,
        Cloud;

        @Override
        public String toString() {
            switch (this) {
                case Local:
                    return "Local";
                case DB:
                    return "DB";
                case Cloud:
                    return "Cloud";
                default:
                    throw new RuntimeException("Unexpected dataset");
            }
        }
    }

    static class ExperimentConfig {
        boolean useRawData;
        boolean usePostProcessing;
        Dataset dataset;
        int machines = 1;

        public String toString() {
            String result = "";
            result += this.dataset.toString();
            result += this.useRawData ? " raw" : " quantized";
            result += this.usePostProcessing ? " noised" : "";
            result += "," + this.machines;
            return result;
        }
    }

    static class ColumnConfig {
        @Nullable
        ColumnQuantization quantization;
        IHistogramBuckets buckets;
        IntervalDecomposition decomposition;
    }

    private ColumnConfig getColumnConfig(ColumnDescription col, ExperimentConfig conf) {
        ColumnConfig result = new ColumnConfig();
        PrivacySchema ps = this.flightsWrapper.getPrivacySchema();
        ColumnQuantization q = ps.quantization(col.name);
        if (conf.useRawData)
            result.quantization = null;
        else
            result.quantization = q;

        if (col.kind.isNumeric() || col.kind == ContentsKind.Date) {
            DoubleColumnQuantization dq = (DoubleColumnQuantization)q;
            assert dq != null;
            DoubleHistogramBuckets b = new DoubleHistogramBuckets(dq.globalMin, dq.globalMax, buckets);
            result.buckets = b;
            result.decomposition = new NumericIntervalDecomposition(dq, b);
        } else if (col.kind.isString()) {
            StringColumnQuantization sq = (StringColumnQuantization)q;
            assert sq != null;
            StringHistogramBuckets b = new StringHistogramBuckets(sq.leftBoundaries);
            result.buckets = b;
            result.decomposition =  new StringIntervalDecomposition(sq, b);
        } else {
            throw new RuntimeException("Column type unsupported in benchmark");
        }
        return result;
    }

    private void benchmarkHeatmap(ExperimentConfig conf, ColumnDescription col0, ColumnDescription col1) {
        IDataSet<ITable> table = conf.dataset == Dataset.Cloud ? Converters.checkNull(this.cloudFlights).get(conf.machines) : this.flights;
        Converters.checkNull(table);

        Function<Heatmap, PrivateHeatmapFactory> postprocess = x -> null;

        PrivacySchema ps = this.flightsWrapper.getPrivacySchema();
        double epsilon = ps.epsilon(col0.name, col1.name);
        ColumnConfig c0 = this.getColumnConfig(col0, conf);
        ColumnConfig c1 = this.getColumnConfig(col1, conf);
        if (conf.usePostProcessing)
            postprocess = x -> new PrivateHeatmapFactory(
                    c0.decomposition, c1.decomposition, x, epsilon, this.flightsWrapper.laplace);

        assert this.ontimeSchema != null;
        String bench = "Heatmap," + col0.name + "+" + col1.name + "," + conf.toString();
        Runnable r;
        Function<Heatmap, PrivateHeatmapFactory> finalPostprocess = postprocess;
        if (conf.dataset == Dataset.DB) {
            r = () -> {
                Heatmap h = this.database.heatmap(col0, col1,
                    c0.buckets, c1.buckets, null, c0.quantization, c1.quantization);
                finalPostprocess.apply(h);
            };
            runNTimes(r, runCount, bench);
        } else {
            ISketch<ITable, Heatmap> hsk = new HeatmapSketch(
                    c0.buckets, c1.buckets, col0.name, col1.name, 1.0, 0, c0.quantization, c1.quantization);
            r = () -> finalPostprocess.apply(table.blockingSketch(hsk));
        }
        quiet = true;
        runNTimes(r, 2, bench);  // warm up jit
        quiet = false;
        runNTimes(r, runCount, bench);
    }

    private void benchmarkHistogram(ExperimentConfig conf, ColumnDescription col) {
        IDataSet<ITable> table = conf.dataset == Dataset.Cloud ?
                Converters.checkNull(this.cloudFlights).get(conf.machines) : this.flights;
        Converters.checkNull(table);

        Function<Histogram, PrivateHistogram> postprocess = x -> null;
        PrivacySchema ps = this.flightsWrapper.getPrivacySchema();
        ColumnConfig c = this.getColumnConfig(col, conf);
        double epsilon = ps.epsilon(col.name);
        if (conf.usePostProcessing)
            postprocess = x -> new PrivateHistogram(c.decomposition, x, epsilon, false, this.flightsWrapper.laplace);

        assert this.ontimeSchema != null;
        String bench = "Histogram," + col.name + "," + conf.toString();
        Runnable r;
        Function<Histogram, PrivateHistogram> finalPostprocess = postprocess;
        if (conf.dataset == Dataset.DB) {
            r = () -> {
                Histogram histo = this.database.histogram(
                    col, c.buckets, null, c.quantization, 0);
                finalPostprocess.apply(histo);
            };
            runNTimes(r, runCount, bench);
        } else {
            ISketch<ITable, Histogram> hsk = new HistogramSketch(
                c.buckets, col.name, 1.0, 0, c.quantization);
            r = () -> finalPostprocess.apply(table.blockingSketch(hsk));
        }
        quiet = true;
        runNTimes(r, 2, bench);  // warm up jit
        quiet = false;
        runNTimes(r, runCount, bench);
    }

    public void run(HashSet<String> datasets) {
        assert this.ontimeSchema != null;
        ExperimentConfig conf = new ExperimentConfig();
        System.out.println("Measurement,Column(s),Type,Machines,Iteration,Time (ms)");
        for (Dataset d: Arrays.asList(Dataset.Cloud, Dataset.Local, Dataset.DB)) {
            if (!datasets.contains(d.toString()))
                continue;

            conf.dataset = d;
            List<Integer> machines = new ArrayList<Integer>();
            if (d.equals(Dataset.Cloud)) {
                assert this.cloudFlights != null;
                machines.addAll(this.cloudFlights.keySet());
            } else {
                machines.add(1);
            }
            for (int m: machines) {
                conf.machines = m;
                List<ColumnDescription> cols = this.ontimeSchema.getColumnDescriptions();
                for (ColumnDescription col : cols) {
                    conf.useRawData = true;
                    conf.usePostProcessing = false;
                    this.benchmarkHistogram(conf, col);
                    conf.useRawData = false;
                    this.benchmarkHistogram(conf, col);
                    conf.usePostProcessing = true;
                    this.benchmarkHistogram(conf, col);
                }
                for (int i = 0; i < cols.size() - 1; i++) {
                    ColumnDescription col0 = cols.get(i);
                    ColumnDescription col1 = cols.get(i + 1);
                    conf.useRawData = true;
                    conf.usePostProcessing = false;
                    this.benchmarkHeatmap(conf, col0, col1);
                    conf.useRawData = false;
                    this.benchmarkHeatmap(conf, col0, col1);
                    conf.usePostProcessing = true;
                    this.benchmarkHeatmap(conf, col0, col1);
                }
            }
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        HillviewLogger.instance.setLogLevel(Level.WARNING);
        HashSet<String> datasets = new HashSet<String>(Arrays.asList(args));
        DPPerfBenchmarks bench = new DPPerfBenchmarks(datasets);
        bench.run(datasets);
    }
}
