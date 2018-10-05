/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.main;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.management.ClusterConfig;
import org.hillview.management.PurgeLeafDatasets;
import org.hillview.management.SetMemoization;
import org.hillview.sketches.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.HostAndPort;
import org.hillview.utils.HostList;
import org.hillview.utils.Parallelizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This code is used to run performance benchmarks.
 * These form a separate main entry point, and should be built into a separate binary.
 * They are meant only for measurements, and are not part of the actual Hillview service.
 */
class Benchmarks {
    private static final ColumnDescription desc = new
            ColumnDescription("SQRT", ContentsKind.Double);

    /**
     * Generates a double array with every fifth entry missing
     */
    @SuppressWarnings("SameParameterValue")
    private static DoubleArrayColumn generateDoubleArray(final int size, final int max) {
        return generateDoubleArray(size, max, 5);
    }

    /**
     * Generates a double array with every skip entry missing
     */
    @SuppressWarnings("SameParameterValue")
    private static DoubleArrayColumn generateDoubleArray(
            final int size, final int max, int skip) {
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i + 1) % max);
            if ((i % skip) == 0)
                col.setMissing(i);
        }
        return col;
    }

    public static class GenerateStringColumnMapper implements IMap<Empty, ITable> {
        private final int distinctValues;
        private final int totalElements;

        GenerateStringColumnMapper(int distinctValues, int totalElements) {
            this.distinctValues = distinctValues;
            this.totalElements = totalElements;
        }

        @Override
        public ITable apply(Empty data) {
            ColumnDescription desc = new ColumnDescription("Strings", ContentsKind.String);
            StringArrayColumn col = new StringArrayColumn(desc, this.totalElements);
            for (int i = 0; i < this.totalElements; i++)
                col.set(i, "s" + Integer.toString(i % this.distinctValues));
            IColumn[] cols = new IColumn[] { col };
            return new Table(cols, null, null);
        }
    }

    private static long time(Runnable runnable) {
        long start = System.nanoTime();
        runnable.run();
        long end = System.nanoTime();
        return end - start;
    }

    private static String twoDigits(double d) {
        return String.format("%.2f", d);
    }

    private static void runNTimes(Runnable runnable, int count, String message, int elemCount) {
        long[] times = new long[count];
        for (int i=0; i < count; i++) {
            long t = time(runnable);
            times[i] = t;
        }
        int minIndex = 0;
        for (int i=0; i < count; i++)
            if (times[i] < times[minIndex])
                minIndex = i;
        System.out.println("Bench,Time (ms),Melems/s,Percent slower");
        for (int i=0; i < count; i++) {
            double speed = (double)(elemCount) / (times[i] / 1000.0);
            double percent = 100 * ((double)times[i] - times[minIndex]) / times[minIndex];
            System.out.println(message + "," +
                    (times[i]/(1000.0 * 1000.0)) + "," +
                    twoDigits(speed) + "," +
                    twoDigits(percent) + "%");
        }
    }

    private static ITable createTable(final int colSize, final IColumn col) {
        FullMembershipSet fMap = new FullMembershipSet(colSize);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, fMap, null, null);
    }

    // Testing the performance of histogram computations
    private static void benchmarkHistogram(
            String[] args) throws IOException, InterruptedException {
        System.out.println(Arrays.toString(args));
        final int runCount = Integer.parseInt(args[1]);
        final int parallelism = Integer.parseInt(args[3]);
        final double rateParameter = Double.parseDouble(args[4]);
        final int datasetScalingParameter = Integer.parseInt(args[5]);
        HillviewLogger.instance.setLogLevel(Level.OFF);
        final int bucketNum = 40;
        final int mega = 1024 * 1024;
        final int colSize = 100 * mega / datasetScalingParameter;
        final DoubleArrayColumn col = generateDoubleArray(colSize, 100);

        IHistogramBuckets buckDes = new DoubleHistogramBuckets(0, 100, bucketNum);
        ITable table = createTable(colSize, col);
        ISketch<ITable, Histogram> sk = new HistogramSketch(
                        buckDes, col.getName(), rateParameter, 0);

        if (args[0].equals("noseparatethread")) {
            final IDataSet<ITable> ds = new LocalDataSet<ITable>(table, false);
            Runnable r = () -> ds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram", colSize);
        }

        if (args[0].equals("separatethread")) {
            final IDataSet<ITable> lds = new LocalDataSet<ITable>(table);
            Runnable r = () -> lds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
        }

        if (args[0].equals("remote")) {
            // Setup server
            final HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1",1234);
            final List<IDataSet<ITable>> tables =  IntStream.range(0, parallelism)
                    .mapToObj((i) -> new LocalDataSet<ITable>(createTable(colSize,
                            generateDoubleArray(colSize, 100))))
                    .collect(Collectors.toList());
            final IDataSet<ITable> lds = new ParallelDataSet<>(tables);
            new HillviewServer(serverAddress, lds);

            // Setup client
            final IDataSet<ITable> remoteIds = new RemoteDataSet<ITable>(serverAddress);
            Runnable r = () -> remoteIds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
        }

        if (args[0].equals("remote-no-memoization")) {
            // Setup server
            final HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1",1234);
            final List<IDataSet<ITable>> tables =  IntStream.range(0, parallelism)
                                    .mapToObj((i) -> new LocalDataSet<ITable>(createTable(colSize,
                                                         generateDoubleArray(colSize, 100))))
                                    .collect(Collectors.toList());
            final IDataSet<ITable> lds = new ParallelDataSet<ITable>(tables);
            final HillviewServer server = new HillviewServer(serverAddress, lds);
            server.setMemoization(false);

            // Setup client
            final IDataSet<ITable> remoteIds = new RemoteDataSet<ITable>(serverAddress);
            Runnable r = () -> remoteIds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
        }


        if (args[0].equals("remote-no-memoization-nw-server")) {
            final HostAndPort serverAddress = HostAndPort.fromParts(args[2],1234);
            // Setup server
            final List<IDataSet<ITable>> tables =  IntStream.range(0, parallelism).parallel()
                    .mapToObj((i) -> {
                        System.out.println("LDS " + i + " " + parallelism);
                        return new LocalDataSet<ITable>(createTable(colSize,
                            generateDoubleArray(colSize, 100)));
                    })
                    .collect(Collectors.toList());
            final IDataSet<ITable> lds = new ParallelDataSet<ITable>(tables);
            final HillviewServer server = new HillviewServer(serverAddress, lds);
            server.setMemoization(false);
            Thread.currentThread().join();
        }

        if (args[0].equals("remote-nw-server")) {
            // Setup server
            final HostAndPort serverAddress = HostAndPort.fromParts(args[2],1234);
            final List<IDataSet<ITable>> tables =  IntStream.range(0, parallelism)
                    .mapToObj((i) -> new LocalDataSet<ITable>(createTable(colSize,
                            generateDoubleArray(colSize, 100))))
                    .collect(Collectors.toList());
            final IDataSet<ITable> lds = new ParallelDataSet<ITable>(tables);
            new HillviewServer(serverAddress, lds);
            Thread.currentThread().join();
        }

        if (args[0].equals("remote-nw-client")) {
            final List<IDataSet<ITable>> dataSets = Arrays.stream(args[2].split(","))
                    .map(s -> s + ":1234")
                    .map(HostAndPort::fromString)
                    .map(RemoteDataSet<ITable>::new)
                    .collect(Collectors.toList());
            // Setup client
            final IDataSet<ITable> remoteIds = new ParallelDataSet<ITable>(dataSets);
            Runnable r = () -> remoteIds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
        }

        System.exit(0);
    }

    // Testing the performance of string quantiles computation
    private static void benchmarkQuantiles(String[] args) throws IOException {
        IDataSet<Empty> original;
        int cores = 2;
        if (false)
            original = new LocalDataSet<Empty>(Empty.getInstance());
        else {
            if (args.length != 1)
                throw new RuntimeException("Expected 1 argument: cluster configuration");
            String file = args[0];
            ClusterConfig config = ClusterConfig.parse(file);
            HostList workers = config.getWorkers();
            original = RemoteDataSet.createCluster(workers, RemoteDataSet.defaultDatasetIndex);
            original = original.blockingFlatMap(new Parallelizer(cores));
        }

        original.blockingManage(new SetMemoization(false));
        SummarySketch summary = new SummarySketch();
        int total = 1000 * 1000 * 10;
        int runCount = 5;
        int quantiles = 100;
        for (int i = 5; i < 24; i++) {
            int distinct = 1 << i;
            GenerateStringColumnMapper generator = new GenerateStringColumnMapper(distinct, total);
            IDataSet<ITable> data = original.blockingMap(generator);
            SummarySketch.TableSummary s = data.blockingSketch(summary);
            System.out.println("Table has " + s.rowCount + " rows");

            ISketch<ITable, DistinctStringsSketch.DistinctStrings> sk =
                    new DistinctStringsSketch("Strings");
            Runnable r = () -> data.blockingSketch(sk).getQuantiles(quantiles);
            runNTimes(r, runCount, "Naive " + distinct + " distinct", total);

            SampleDistinctElementsSketch scs = new SampleDistinctElementsSketch("Strings", 0, quantiles);
            r = () -> data.blockingSketch(scs).getLeftBoundaries(100);
            runNTimes(r, runCount, "Smart " + distinct + " distinct", total);
        }
        original.blockingManage(new PurgeLeafDatasets());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HillviewLogger.instance.setLogLevel(Level.WARNING);
        if (false)
            benchmarkHistogram(args);
        else
            benchmarkQuantiles(args);
    }
}
