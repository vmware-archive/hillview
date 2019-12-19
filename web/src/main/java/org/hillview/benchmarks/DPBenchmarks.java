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

import org.hillview.dataStructures.DyadicDecomposition;
import org.hillview.dataStructures.NumericDyadicDecomposition;
import org.hillview.dataStructures.PrivateHeatmap;
import org.hillview.dataStructures.PrivateHistogram;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.main.Benchmarks;
import org.hillview.management.ClusterConfig;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.DoubleDataRangeSketch;
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
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.targets.DPWrapper;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.HostList;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * This is a main class for running various differential privacy benchmarks.
 * The measurements are run headless.
 */
public class DPBenchmarks extends Benchmarks {
    private static int runCount = 5;
    private static int buckets = 100;
    @Nullable
    private Schema ontimeSchema;

    @Nullable
    private IDataSet<ITable> flights;
    @Nullable
    private IDataSet<ITable> cloudFlights; // flights dataset in the cloud
    private DPWrapper flightsWrapper;
    private JdbcDatabase database;

    private DPBenchmarks(HashSet<String> datasets) throws SQLException, IOException {
        this.loadData(datasets);
    }

    private void loadData(HashSet<String> datasets) throws SQLException, IOException {
        this.ontimeSchema = Schema.readFromJsonFile(Paths.get("data/ontime/short.schema"));
        String privacyMetadataFile = DPWrapper.privacyMetadataFile("data/ontime_private");
        assert privacyMetadataFile != null;
        PrivacySchema ps = PrivacySchema.loadFromFile(privacyMetadataFile);
        this.flightsWrapper = new DPWrapper(ps, privacyMetadataFile);

        if (datasets.contains("DB")) {
            System.out.println("Loading database");
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
            System.out.println("Loading dataset");
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
            IDataSet<Empty> clusterInit = RemoteDataSet.createCluster(workers, RemoteDataSet.defaultDatasetIndex);
            FileSetDescription desc = new FileSetDescription();
            desc.fileKind = "orc";
            desc.fileNamePattern = "data/ontime_small_orc/*.orc";
            desc.schemaFile = "schema";
            this.cloudFlights = this.loadTable(clusterInit, desc);
            IDataSet<ITable> data = this.cloudFlights;
            SummarySketch sk = new SummarySketch();
            TableSummary tableSummary = data.blockingSketch(sk);
            assert tableSummary != null;
            if (tableSummary.rowCount == 0)
                throw new RuntimeException("No file data loaded");
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
        boolean usePublicData;
        boolean usePostProcessing;
        Dataset dataset;

        public String toString() {
            String result = "";
            result += this.dataset.toString();
            result += this.usePublicData ? " public" : " private";
            result += this.usePostProcessing ? " noised" : "";
            return result;
        }
    }

    private double benchmarkNumericHeatmap(ExperimentConfig conf, String col0, String col1) {
        IDataSet<ITable> table = conf.dataset == Dataset.Cloud ? this.cloudFlights : this.flights;
        Converters.checkNull(table);

        DoubleDataRangeSketch drsk = new DoubleDataRangeSketch(col0);
        DataRange dataRange0 = table.blockingSketch(drsk);
        assert dataRange0 != null;
        drsk = new DoubleDataRangeSketch(col1);
        DataRange dataRange1 = table.blockingSketch(drsk);
        assert dataRange1 != null;

        DoubleHistogramBuckets buckDes0 = new DoubleHistogramBuckets(
            dataRange0.min, dataRange0.max, buckets);
        DoubleHistogramBuckets buckDes1 = new DoubleHistogramBuckets(
            dataRange1.min, dataRange1.max, buckets);

        ColumnQuantization q0 = null;
        ColumnQuantization q1 = null;
        Function<Heatmap, PrivateHeatmap> postprocess = x -> null;
        if (!conf.usePublicData) {
            PrivacySchema ps = this.flightsWrapper.getPrivacySchema();
            q0 = ps.quantization(col0);
            q1 = ps.quantization(col1);
            double epsilon = ps.epsilon(col0, col1);
            assert q0 != null;
            assert q1 != null;
            DyadicDecomposition d0 = new NumericDyadicDecomposition(
                (DoubleColumnQuantization)q0, buckDes0);
            DyadicDecomposition d1 = new NumericDyadicDecomposition(
                (DoubleColumnQuantization)q0, buckDes0);
            if (conf.usePostProcessing)
                postprocess = x -> new PrivateHeatmap(d0, d1, x, epsilon, this.flightsWrapper.laplace);
        }

        assert this.ontimeSchema != null;
        ColumnDescription desc0 = this.ontimeSchema.getDescription(col0);
        ColumnDescription desc1 = this.ontimeSchema.getDescription(col1);
        System.out.println("Bench,Time (ms),Melems/s,Percent slower");
        String bench = "Heatmap " + col0 + "," + col1 + "," + conf.toString();
        Runnable r;
        Function<Heatmap, PrivateHeatmap> finalPostprocess = postprocess;
        if (conf.dataset == Dataset.DB) {
            ColumnQuantization finalQ0 = q0;
            ColumnQuantization finalQ1 = q1;
            r = () -> {
                Heatmap h = this.database.heatmap(desc0, desc1,
                    buckDes0, buckDes1, null, finalQ0, finalQ1);
                finalPostprocess.apply(h);
            };
            return runNTimes(r, runCount, bench, 0);
        } else {
            ISketch<ITable, Heatmap> hsk = new HeatmapSketch(
                buckDes0, buckDes1, col0, col1, 1.0, 0, q0, q1);
            r = () -> finalPostprocess.apply(table.blockingSketch(hsk));
        }
        return runNTimes(r, runCount, bench, 0);
    }

    private double benchmarkNumericHistogram(ExperimentConfig conf, String col) {
        IDataSet<ITable> table = conf.dataset == Dataset.Cloud ? this.cloudFlights : this.flights;
        Converters.checkNull(table);
        DoubleDataRangeSketch drsk = new DoubleDataRangeSketch(col);
        DataRange dataRange = table.blockingSketch(drsk);
        assert dataRange != null;

        DoubleHistogramBuckets buckDes = new DoubleHistogramBuckets(
            dataRange.min, dataRange.max, buckets);
        ColumnQuantization q = null;
        Function<Histogram, PrivateHistogram> postprocess = x -> null;
        if (!conf.usePublicData) {
            PrivacySchema ps = this.flightsWrapper.getPrivacySchema();
            q = ps.quantization(col);
            double epsilon = ps.epsilon(col);
            assert q != null;
            DyadicDecomposition d = new NumericDyadicDecomposition(
                (DoubleColumnQuantization)q, buckDes);
            if (conf.usePostProcessing)
                postprocess = x -> new PrivateHistogram(d, x, epsilon, false, this.flightsWrapper.laplace);
        }

        assert this.ontimeSchema != null;
        ColumnDescription desc = this.ontimeSchema.getDescription(col);
        System.out.println("Bench,Time (ms),Melems/s,Percent slower");
        String bench = "Histogram " + col + "," + conf.toString();
        Runnable r;
        Function<Histogram, PrivateHistogram> finalPostprocess = postprocess;
        if (conf.dataset == Dataset.DB) {
            ColumnQuantization finalQ = q;
            r = () -> {
                Histogram histo = this.database.histogram(
                    desc, buckDes, null, finalQ, 0);
                finalPostprocess.apply(histo);
            };
            return runNTimes(r, runCount, bench, 0);
        } else {
            ISketch<ITable, Histogram> hsk = new HistogramSketch(
                buckDes, col, 1.0, 0, q);
            r = () -> finalPostprocess.apply(table.blockingSketch(hsk));
        }
        return runNTimes(r, runCount, bench, 0);
    }

    public void run(HashSet<String> datasets) {
        assert this.ontimeSchema != null;
        ExperimentConfig conf = new ExperimentConfig();
        for (Dataset d: Arrays.asList(Dataset.Cloud, Dataset.Local, Dataset.DB)) {
            if (!datasets.contains(d.toString()))
                continue;

            conf.dataset = d;
            List<ColumnDescription> cols = this.ontimeSchema.getColumnDescriptions();
            cols = Linq.where(cols, c -> c.kind.isNumeric());
            for (ColumnDescription col: cols) {
                conf.usePublicData = true;
                conf.usePostProcessing = false;
                double pub = this.benchmarkNumericHistogram(conf, col.name);
                conf.usePublicData = false;
                double pri0 = this.benchmarkNumericHistogram(conf, col.name);
                conf.usePostProcessing = true;
                double pri1 = this.benchmarkNumericHistogram(conf, col.name);
                System.out.println("Slowdown of quantized=" + Math.round(pri0 / pub * 100) + "%");
                System.out.println("Slowdown of private=" + Math.round(pri1 / pub * 100) + "%");
            }
            for (int i = 0; i < cols.size() - 1; i++) {
                String col0 = cols.get(i).name;
                String col1 = cols.get(i + 1).name;
                conf.usePublicData = true;
                conf.usePostProcessing = false;
                double pub = this.benchmarkNumericHeatmap(conf, col0, col1);
                conf.usePublicData = false;
                double pri0 = this.benchmarkNumericHeatmap(conf, col0, col1);
                conf.usePostProcessing = true;
                double pri1 = this.benchmarkNumericHeatmap(conf, col0, col1);
                System.out.println("Slowdown of quantized=" + Math.round(pri0 / pub * 100) + "%");
                System.out.println("Slowdown of private=" + Math.round(pri1 / pub * 100) + "%");
            }
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        HillviewLogger.instance.setLogLevel(Level.WARNING);
        HashSet<String> datasets = new HashSet<String>(Arrays.asList(args));
        DPBenchmarks bench = new DPBenchmarks(datasets);
        bench.run(datasets);
    }
}
