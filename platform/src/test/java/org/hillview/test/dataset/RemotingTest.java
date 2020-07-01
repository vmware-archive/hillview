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

package org.hillview.test.dataset;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.monoids.PartialResultMonoid;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.maps.FalseMap;
import org.hillview.sketches.NextKSketch;
import org.hillview.sketches.SampleQuantileSketch;
import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.sketches.results.NextKList;
import org.hillview.sketches.results.SampleList;
import org.hillview.table.ColumnDescription;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.test.BaseTest;
import org.hillview.utils.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Remoting tests for Hillview.
 */
@SuppressWarnings("BusyWait")
@net.jcip.annotations.NotThreadSafe
public class RemotingTest extends BaseTest {
    // We have to be careful to use different ports for all the servers we start
    // Although there's a NotThreadSafe annotation it does not seem to have much effect.
    private final static HostAndPort serverAddress =
            HostAndPort.fromParts("127.0.0.1", 1239);
    @Nullable private static HillviewServer server;

    private static class IncrementMap implements IMap<int[], int[]> {
        static final long serialVersionUID = 1;

        @Override
        public int[] apply(final int[] data) {
            Assert.assertNotNull(data);
            if (data.length == 0) {
                throw new RuntimeException("Cannot apply map against empty data");
            }

            final int[] dataNew = new int[data.length];
            for (int i = 0; i < data.length; i++) {
                dataNew[i] = data[i] + 1;
            }

            return dataNew;
        }
    }

    private static class SumSketch implements ISketch<int[], DataSetTest.IntegerWrapper> {
        static final long serialVersionUID = 1;
        @Override @Nullable
        public DataSetTest.IntegerWrapper zero() {
            return new DataSetTest.IntegerWrapper(0);
        }

        @Override @Nullable
        public DataSetTest.IntegerWrapper add(@Nullable final DataSetTest.IntegerWrapper left, @Nullable final DataSetTest.IntegerWrapper right) {
            return new DataSetTest.IntegerWrapper(Converters.checkNull(left).value + Converters.checkNull(right).value);
        }

        @Override
        public DataSetTest.IntegerWrapper create(final int[] data) {
            int sum = 0;
            Assert.assertNotNull(data);
            for (int d : data) sum += d;
            return new DataSetTest.IntegerWrapper(sum);
        }
    }

    private static class ImmutableListSketch implements ISketch<int[], JsonList<Integer>> {
        static final long serialVersionUID = 1;
        @Override @Nullable
        public JsonList<Integer> zero() {
            return new JsonList<Integer>();
        }

        @Override @Nullable
        public JsonList<Integer> add(@Nullable final JsonList<Integer> left,
                                     @Nullable final JsonList<Integer> right) {
            Converters.checkNull(left);
            JsonList<Integer> result = new JsonList<Integer>(left);
            result.addAll(Converters.checkNull(right));
            return result;
        }

        @Override
        public JsonList<Integer> create(final int[] data) {
            JsonList<Integer> result = new JsonList<Integer>();
            result.add(1);
            return result;
        }
    }

    private static class ErrorSumSketch implements ISketch<int[], DataSetTest.IntegerWrapper> {
        static final long serialVersionUID = 1;
        @Override @Nullable
        public DataSetTest.IntegerWrapper zero() {
            return new DataSetTest.IntegerWrapper(0);
        }

        @Override @Nullable
        public DataSetTest.IntegerWrapper add(@Nullable final DataSetTest.IntegerWrapper left, @Nullable final DataSetTest.IntegerWrapper right) {
            return new DataSetTest.IntegerWrapper(Converters.checkNull(left).value + Converters.checkNull(right).value);
        }

        @Override
        public DataSetTest.IntegerWrapper create(final int[] data) {
            throw new RuntimeException("ErrorSumSketch");
        }
    }

    /*
     * Create separate server and client actor systems to test remoting.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        // Server
        final int parts = 10;
        final int size = 1000;
        ArrayList<IDataSet<int[]>> al = new ArrayList<IDataSet<int[]>>(10);
        for (int i=0; i < parts; i++) {
            final int[] data = new int[size];
            for (int j = 0; j < size; j++)
                data[j] = (i * size) + j;
            LocalDataSet<int[]> lds = new LocalDataSet<int[]>(data);
            al.add(lds);
        }
        ParallelDataSet<int[]> pds = new ParallelDataSet<int[]>(al);
        pds.setBundleInterval(0);
        server = new HillviewServer(serverAddress, pds);
    }

    @Test
    public void testMapSketchThroughClient() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                                                      .filter(p -> p.deltaValue != null)
                                                      .toBlocking()
                                                      .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final int result = remoteIdsNew.sketch(new SumSketch())
                                       .map(e -> Converters.checkNull(e.deltaValue).value)
                                       .reduce(Integer::sum)
                                       .toBlocking()
                                       .last();
        assertEquals(50005000, result);
    }

    //@Test
    public void testRaceSerialized() throws InterruptedException {
        final ExecutorService es = Executors.newFixedThreadPool(10);
        final SerializedSubject<Integer, Integer> obs1 = PublishSubject.<Integer>create().toSerialized();
        final SerializedSubject<Integer, Integer> obs2 = PublishSubject.<Integer>create().toSerialized();
        final Observable<List<Integer>> merge = Observable.merge(obs1, obs2)
                .buffer(100, TimeUnit.MILLISECONDS)
                .filter(e -> !e.isEmpty());

        merge.subscribe(new Subscriber<List<Integer>>() {
            int interrupted = 0;

            @Override
            public void onCompleted() {
                Assert.assertNotEquals(interrupted, 0);
            }

            @Override
            public void onError(final Throwable throwable) { }

            @Override
            public void onNext(final List<Integer> integer) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    this.interrupted++;
                }
            }
        });
        es.execute(() -> obs1.onNext(1));
        es.execute(() -> obs2.onNext(2));
        es.execute(() -> obs1.onNext(3));
        es.execute(() -> obs2.onNext(4));
        Thread.sleep(100);
        obs1.onCompleted();
        obs2.onCompleted();
    }

    @Test
    public void testMapSketchThroughClientUsingMemoization() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                .filter(p -> p.deltaValue != null)
                .toBlocking()
                .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final int result = remoteIdsNew.sketch(new SumSketch())
                .map(e -> Converters.checkNull(e.deltaValue).value)
                .reduce(Integer::sum)
                .toBlocking()
                .last();
        assertEquals(50005000, result);

        final IDataSet<int[]> remoteIdsNewMem = remoteIds.map(new IncrementMap())
                .filter(p -> p.deltaValue != null)
                .toBlocking()
                .last().deltaValue;
        assertNotNull(remoteIdsNewMem);
        final int memoizedResult = remoteIdsNew.sketch(new SumSketch())
                .map(e -> Converters.checkNull(e.deltaValue).value)
                .reduce(Integer::sum)
                .toBlocking()
                .last();
        assertEquals(50005000, memoizedResult);
    }

    static class MakeEmpty implements IMap<int[], Empty> {
        static final long serialVersionUID = 1;
        
        @Nullable
        @Override
        public Empty apply(@Nullable int[] data) {
            return Empty.getInstance();
        }
    }

    static class IsEmpty implements IMap<Empty, Boolean> {
        static final long serialVersionUID = 1;

        @Nullable
        @Override
        public Boolean apply(@Nullable Empty data) {
            return true;
        }
    }

    //@Test
    // TODO: currently prune does not seem to work as expected.
    public void testPrune() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        Observable<PartialResult<IDataSet<int[]>>> pruned = remoteIds.prune(new FalseMap<int[]>());
        TestSubscriber<PartialResult<IDataSet<int[]>>> ts = new TestSubscriber<PartialResult<IDataSet<int[]>>>();
        pruned.toBlocking().subscribe(ts);
        ts.assertValueCount(1);

        final IDataSet<Empty> empty = remoteIds.map(new MakeEmpty())
                .filter(p -> p.deltaValue != null)
                .toBlocking()
                .last().deltaValue;
        assertNotNull(empty);
        Observable<PartialResult<IDataSet<Empty>>> pruned2 = empty.prune(new IsEmpty());
        TestSubscriber<PartialResult<IDataSet<Empty>>> ts2 = new TestSubscriber<PartialResult<IDataSet<Empty>>>();
        pruned2.toBlocking().subscribe(ts2);
        ts2.assertValueCount(1);
    }

    //@Test
    public void testMapSketchThroughClientWithError() {
        // This test hangs intermittently so it has been disabled
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                                                      .toBlocking()
                                                      .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final Observable<PartialResult<DataSetTest.IntegerWrapper>> resultObs =
                remoteIdsNew.sketch(new ErrorSumSketch());
        TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>> ts = new TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>>();
        resultObs.toBlocking().subscribe(ts);
        ts.assertError(RuntimeException.class);
    }

    @Test
    public void testMapSketchThroughClientWithImmutableCollection() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                                                      .toBlocking()
                                                      .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final Observable<PartialResult<JsonList<Integer>>> resultObs =
                remoteIdsNew.sketch(new ImmutableListSketch());
        final List<Integer> result = resultObs.map(e -> e.deltaValue)
                                              .toBlocking()
                                              .last();
        for (int val: result) {
            assertEquals(1, val);
        }
    }

    private static TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>> createUnsubscribeSubscriber(int count) {
        return new TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>>() {
            private int counter = 0;

            @Override
            public void onNext(final PartialResult<DataSetTest.IntegerWrapper> pr) {
                this.counter++;
                super.onNext(pr);
                if (this.counter == count)
                    this.unsubscribe();
            }
        };
    }

    @Test
    public void testUnsubscribe() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final Observable<PartialResult<DataSetTest.IntegerWrapper>> resultObs = remoteIds.sketch(new SumSketch());
        TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>> ts = createUnsubscribeSubscriber(1);
        resultObs.toBlocking().subscribe(ts);
        ts.assertValueCount(1);
        ts.assertNotCompleted();
    }

    @Test
    public void testDoOnUnsubscribeByCaller() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final AtomicInteger count = new AtomicInteger(0);
        final Observable<PartialResult<DataSetTest.IntegerWrapper>> resultObs =
                remoteIds.sketch(new SumSketch()).doOnUnsubscribe(count::incrementAndGet);
        TestSubscriber<PartialResult<DataSetTest.IntegerWrapper>> ts = createUnsubscribeSubscriber(3);
        resultObs.toBlocking().subscribe(ts);
        ts.assertValueCount(3);
        ts.assertNotCompleted();
        assertEquals(1, count.get());
    }

    @Test
    public void testZip() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress);
        final IDataSet<int[]> remoteIdsLeft = Converters.checkNull(
                remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue);
        final IDataSet<int[]> remoteIdsRight = Converters.checkNull(
                remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue);
        final PartialResult<IDataSet<Pair<int[], int[]>>> last
                = Converters.checkNull(remoteIdsLeft.zip(remoteIdsRight)).toBlocking().last();
        assertNotNull(last);
        assertEquals(last.deltaDone, 1.0, 0.001);
    }

    @Test
    public void testIncorrectRemoteIndex() {
        try {
            // Should not succeed because the remote handle does not exist
            int nonExistentIndex = 99;
            final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(serverAddress,
                                                                       nonExistentIndex);
            Converters.checkNull(
                    remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue);
            fail();
        }
        catch (RuntimeException ignored) {
        }
        try {
            // Test with zip
            final IDataSet<int[]> remoteIdsLeft = new RemoteDataSet<int[]>(serverAddress);
            final IDataSet<int[]> remoteIdsRight = new RemoteDataSet<int[]>(serverAddress, 99);
            Converters.checkNull(remoteIdsLeft.zip(remoteIdsRight)).toBlocking().last();
            fail();
        } catch (RuntimeException ignored) {
        }
    }

    @AfterClass
    public static void shutdown() {
        if (server != null) {
            server.shutdown();
        }
    }

    // The following are testing unsubscription
    private static final AtomicLong initialTime = new AtomicLong(0);
    private static final AtomicInteger completedLocals = new AtomicInteger(0);

    private static long getTime() {
        long time = System.currentTimeMillis();
        initialTime.compareAndSet(0, time);
        return time - initialTime.get();
    }

    private static final boolean quiet = true;
    private static void print(String s) {
        if (quiet)
            return;
        System.out.println(s);
    }

    static class SlowSketch implements ISketch<Integer, DataSetTest.IntegerWrapper> {
        static final long serialVersionUID = 1;
        @Override
        public DataSetTest.IntegerWrapper create(Integer data) {
            print(getTime() + " working " + data);
            int sum = 0;
            for (int i = 0; i < 10000000; i++)
                sum = (sum + i) % 43;
            print(getTime() + " ready " + data + ": " + sum);
            completedLocals.getAndIncrement();
            return new DataSetTest.IntegerWrapper(0);
        }

        @Nullable
        @Override
        public DataSetTest.IntegerWrapper zero() {
            return new DataSetTest.IntegerWrapper(0);
        }

        @Nullable
        @Override
        public DataSetTest.IntegerWrapper add(@Nullable DataSetTest.IntegerWrapper left, @Nullable DataSetTest.IntegerWrapper right) {
            return new DataSetTest.IntegerWrapper(0);
        }
    }

    static class SlowMap implements IMap<Integer, Integer> {
        static final long serialVersionUID = 1;
        @Override
        public Integer apply(Integer data) {
            print(getTime() + " working " + data);
            int sum = 0;
            for (int i = 0; i < 10000000; i++)
                sum = (sum + i) % 43;
            print(getTime() + " ready " + data + ": " + sum);
            completedLocals.getAndIncrement();
            return sum;
        }
    }

    /**
     * Test unsubscription through a RemoteDataSet.
     * There were several races which prevented unsubscription from cancelling
     * the remote operation.  Unfortunately it is not easy to write a fully
     * reproducible test.
     */
    @Test
    public void unsubscriptionTest1() throws InterruptedException, IOException {
        ArrayList<IDataSet<Integer>> l = new ArrayList<IDataSet<Integer>>(100);
        for (int j = 0; j < 100; j++) {
            LocalDataSet<Integer> ld = new LocalDataSet<Integer>(j, true);
            l.add(ld);
        }
        ParallelDataSet<Integer> ld = new ParallelDataSet<Integer>(l);

        HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1", 1240);
        HillviewServer server = new HillviewServer(serverAddress, ld);
        IDataSet<Integer> remote = new RemoteDataSet<Integer>(serverAddress);

        SlowSketch sketch = new SlowSketch();
        PartialResultMonoid<DataSetTest.IntegerWrapper> prm = new PartialResultMonoid<DataSetTest.IntegerWrapper>(sketch);

        for (int i = 0; i < 2; i++) {
            completedLocals.set(0);
            IDataSet<Integer> toTest = i == 0 ? ld : remote;
            Observable<PartialResult<DataSetTest.IntegerWrapper>> src = toTest.sketch(sketch).scan(prm::add);
            Observer<PartialResult<DataSetTest.IntegerWrapper>> obs = new Observer<PartialResult<DataSetTest.IntegerWrapper>>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onNext(PartialResult<DataSetTest.IntegerWrapper> i) {
                }
            };
            src = src.doOnUnsubscribe(() -> print(getTime() + " unsubscribed"));

            print(getTime() + " starting");
            Subscription sub = src.subscribe(obs);
            print(getTime() + " sleeping");
            Thread.sleep(200);
            print(getTime() + " unsubscribing");
            sub.unsubscribe();
            print(getTime() + " Sleeping some more");
            Thread.sleep(3000);
            print("Completed locals: " + completedLocals.get());

            while (!sub.isUnsubscribed())
                Thread.sleep(50);
        }

        server.shutdown();
    }

    /**
     * Test unsubscription through a RemoteDataSet.
     * There were several races which prevented unsubscription from cancelling
     * the remote operation.  Unfortunately it is not easy to write a fully
     * reproducible test.
     */
    @Test
    public void unsubscriptionTest2() throws InterruptedException, IOException {
        ArrayList<IDataSet<Integer>> l = new ArrayList<IDataSet<Integer>>(100);
        for (int j = 0; j < 100; j++) {
            LocalDataSet<Integer> ld = new LocalDataSet<Integer>(j, true);
            l.add(ld);
        }
        ParallelDataSet<Integer> ld = new ParallelDataSet<Integer>(l);

        HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1", 1241);
        HillviewServer server = new HillviewServer(serverAddress, ld);
        IDataSet<Integer> remote = new RemoteDataSet<Integer>(serverAddress);
        SlowMap sketch = new SlowMap();

        for (int i = 0; i < 2; i++) {
            completedLocals.set(0);
            IDataSet<Integer> toTest = i == 0 ? ld : remote;
            Observable<PartialResult<IDataSet<Integer>>> src = toTest.map(sketch);
            Observer<PartialResult<IDataSet<Integer>>> obs =
                    new Observer<PartialResult<IDataSet<Integer>>>() {
                        @Override
                        public void onCompleted() {
                        }

                        @Override
                        public void onError(Throwable throwable) {
                        }

                        @Override
                        public void onNext(PartialResult<IDataSet<Integer>> i) {
                        }
                    };
            src = src.doOnUnsubscribe(() -> print(getTime() + " unsubscribed"));

            print(getTime() + " starting");
            Subscription sub = src.subscribe(obs);
            print(getTime() + " sleeping");
            Thread.sleep(200);
            print(getTime() + " unsubscribing");
            sub.unsubscribe();
            print(getTime() + " Sleeping some more");
            Thread.sleep(5000);
            print("Completed locals: " + completedLocals.get());

            while (!sub.isUnsubscribed())
                Thread.sleep(50);
        }
        server.shutdown();
    }

    @Test
    public void remoteDataSetTest() throws IOException {
        int numCols = 3;
        int size = 1000, resolution = 20;
        SmallTable randTable = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size, 0);
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1234);
        HillviewServer server1 = new HillviewServer(h1, new LocalDataSet<ITable>(randTable));
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            SampleList sl = rds1.blockingSketch(sqSketch);
            Assert.assertNotNull(sl);
            IndexComparator comp = cso.getIndexComparator(sl.table);
            for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
                assertTrue(comp.compare(i, i + 1) <= 0);
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest1() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Column0", ContentsKind.Integer), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1235);
        ArrayList<IDataSet<ITable>> one = new ArrayList<IDataSet<ITable>>();
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(TestTables.getIntTable(20, 2));
        one.add(local);
        IDataSet<ITable> small = new ParallelDataSet<ITable>(one);
        HillviewServer server1 = new HillviewServer(h1, small);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = rds1.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x10]", sl.rows.toString());
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest2() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Name", ContentsKind.String), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1236);
        ArrayList<IDataSet<ITable>> empty = new ArrayList<IDataSet<ITable>>();
        IDataSet<ITable> small = new ParallelDataSet<ITable>(empty);
        HillviewServer server1 = new HillviewServer(h1, small);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = rds1.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x0]", sl.rows.toString());
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest3() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Name", ContentsKind.String), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1237);
        HostAndPort h2 = HostAndPort.fromParts("127.0.0.1", 1238);

        ArrayList<IDataSet<ITable>> one = new ArrayList<IDataSet<ITable>>();
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(TestTables.testTable());
        one.add(local);
        IDataSet<ITable> small = new ParallelDataSet<ITable>(one);
        HillviewServer server1 = new HillviewServer(h1, small);

        ArrayList<IDataSet<ITable>> none = new ArrayList<IDataSet<ITable>>();
        IDataSet<ITable> empty = new ParallelDataSet<ITable>(none);
        HillviewServer server2 = new HillviewServer(h2, empty);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            RemoteDataSet<ITable> rds2 = new RemoteDataSet<ITable>(h2);
            ArrayList<IDataSet<ITable>> two = new ArrayList<IDataSet<ITable>>();
            two.add(rds1);
            two.add(rds2);
            ParallelDataSet<ITable> top = new ParallelDataSet<ITable>(two);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = top.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x10]", sl.rows.toString());
        } finally {
            server1.shutdown();
            server2.shutdown();
        }
    }
}
