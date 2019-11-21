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
import org.hillview.dataStructures.PrivateHistogram;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.main.Benchmarks;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.DoubleDataRangeSketch;
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
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * This is a main class for running various differential privacy benchmarks.
 * The measurements are run headless.
 */
@SuppressWarnings("FieldCanBeLocal")
public class DPBenchmarks extends Benchmarks {
    private static int runCount = 10;
    private static boolean local = true;
    @Nullable
    private Schema ontimeSchema;

    @Nullable
    private IDataSet<ITable> flights;
    private LocalDataSet<Empty> start = new LocalDataSet<Empty>(Empty.getInstance());
    private DPWrapper flightsWrapper;
    private JdbcDatabase database;

    private DPBenchmarks() throws SQLException {
        this.start = new LocalDataSet<Empty>(Empty.getInstance());
        this.loadData();
    }

    private void loadData() throws SQLException {
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

        FileSetDescription desc = new FileSetDescription();
        desc.fileKind = "csv";
        desc.headerRow = true;
        desc.fileNamePattern = "data/ontime/2017*.csv.gz";
        desc.schemaFile = "short.schema";
        System.out.println("Loading dataset");
        this.flights = this.loadTable(desc);
        String privacyMetadataFile = DPWrapper.privacyMetadataFile("data/ontime_private");
        assert privacyMetadataFile != null;
        PrivacySchema ps = PrivacySchema.loadFromFile(privacyMetadataFile);
        this.flightsWrapper = new DPWrapper(ps);
        this.ontimeSchema = Schema.readFromJsonFile(Paths.get("data/ontime/short.schema"));
    }

    private IDataSet<ITable> loadTable(FileSetDescription desc) {
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(desc);
        IDataSet<IFileReference> found = start.blockingFlatMap(finder);
        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        return found.blockingMap(loader);
    }

    static class ExperimentConfig {
        boolean usePublicData;
        boolean usePostProcessing;
        boolean useDatabase;

        public String toString() {
            String result = "";
            result += this.useDatabase ? "DB " : "files ";
            result += this.usePublicData ? "public " : "private ";
            result += this.usePostProcessing ? "noised " : "";
            return result;
        }
    }

    private double benchmarkNumericHistogram(ExperimentConfig conf, String col) {
        IDataSet<ITable> data = this.flights;
        assert data != null;
        SummarySketch sk = new SummarySketch();
        TableSummary tableSummary = data.blockingSketch(sk);
        assert tableSummary != null;
        long size = tableSummary.rowCount;

        DoubleDataRangeSketch drsk = new DoubleDataRangeSketch(col);
        DataRange dataRange = data.blockingSketch(drsk);
        assert dataRange != null;

        int bucketNum = 50;
        DoubleHistogramBuckets buckDes = new DoubleHistogramBuckets(
            dataRange.min, dataRange.max, bucketNum);

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
                postprocess = x -> new PrivateHistogram(d, x, epsilon, false);
        }

        assert this.ontimeSchema != null;
        ColumnDescription desc = this.ontimeSchema.getDescription(col);
        System.out.println("Bench,Time (ms),Melems/s,Percent slower");
        String bench = "Histogram " + col + conf.toString();
        Runnable r;
        Function<Histogram, PrivateHistogram> finalPostprocess = postprocess;
        if (conf.useDatabase) {
            ColumnQuantization finalQ = q;
            r = () -> {
                Histogram histo = this.database.histogram(
                    desc, buckDes, null, finalQ, (int)size);
                finalPostprocess.apply(histo);
            };
            return runNTimes(r, runCount, bench, size);
        } else {
            ISketch<ITable, Histogram> hsk = new HistogramSketch(
                buckDes, col, 1.0, 0, q);
            r = () -> finalPostprocess.apply(data.blockingSketch(hsk));
        }
        return runNTimes(r, runCount, bench, size);
    }

    public void run() {
        assert this.ontimeSchema != null;
        ExperimentConfig conf = new ExperimentConfig();
        conf.useDatabase = true;
        for (ColumnDescription col: this.ontimeSchema.getColumnDescriptions()) {
            if (col.kind.isNumeric()) {
                conf.usePublicData = true;
                conf.usePostProcessing = false;
                double pub = this.benchmarkNumericHistogram(conf, col.name);
                conf.usePublicData = false;
                double pri0 = this.benchmarkNumericHistogram(conf, col.name);
                conf.usePostProcessing = true;
                double pri1 = this.benchmarkNumericHistogram(conf, col.name);
                System.out.println("Slowdown=" + Math.round(pri0 / pub * 100) + "%");
                System.out.println("Slowdown w post=" + Math.round(pri1 / pub * 100) + "%");
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        HillviewLogger.instance.setLogLevel(Level.WARNING);
        DPBenchmarks bench = new DPBenchmarks();
        bench.run();
    }
}
