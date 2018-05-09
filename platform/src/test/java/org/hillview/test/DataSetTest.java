/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

package org.hillview.test;

import com.google.common.net.HostAndPort;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.PartialResultMonoid;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.observers.TestSubscriber;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DataSetTest extends BaseTest {
    private class Increment implements IMap<Integer, Integer> {
        @Override
        public Integer apply(final @Nullable Integer data) {
            return Converters.checkNull(data) + 1;
        }
    }

    private class Sketch implements ISketch<Integer, Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(@Nullable final Integer left, @Nullable final Integer right) {
            Assert.assertNotNull(left);
            Assert.assertNotNull(right);
            return left + right;
        }

        @Override
        public Integer create(@Nullable final Integer data) {
            return Converters.checkNull(data);
        }
    }

    @Test
    public void localDataSetTest() {
        final LocalDataSet<Integer> ld = new LocalDataSet<Integer>(4);
        final Increment increment = new Increment();

        final IDataSet<Integer> r = ld.blockingMap(increment);
        final Sketch sketch = new Sketch();
        final int result = r.blockingSketch(sketch);
        Assert.assertEquals(result, 5);
    }

    @Test
    public void idempotenceTest() {
        int[] count = new int[1];
        count[0] = 0;

        LocalDataSet<Integer> local = new LocalDataSet<Integer>(10);
        List<IDataSet<Integer>> l = new ArrayList<IDataSet<Integer>>();
        l.add(local);
        ParallelDataSet<Integer> pds = new ParallelDataSet<Integer>(l);
        IMap<Integer, Integer> map = (IMap<Integer, Integer>) data -> {
            count[0]++;
            return data + 1;
        };
        IDataSet<Integer> r = local.blockingMap(map);
        Assert.assertNotNull(r);
        Assert.assertEquals(count[0], 1);

        r = pds.blockingMap(map);
        Assert.assertNotNull(r);
        Assert.assertEquals(count[0], 2);
    }

    // This test explores the semantics of publish/connect.
    @Test
    public void rxJavaTest() {
        Observable<String> o = Observable
                .just("a", "b", "c")
                .map(s -> s)
                .publish()
                .autoConnect(2);

        Observable<String> o1 = o.map(s -> s + "0").first().single();
        Observable<String> o2 = o.map(s -> s + "1");
        Observable<String> m = o1.mergeWith(o2);
        m.subscribe(s -> System.out.println("Sub1 got: " + s));
    }

    @Test
    public void parallelDataSetTest() {
        final LocalDataSet<Integer> ld1 = new LocalDataSet<Integer>(4);
        final LocalDataSet<Integer> ld2 = new LocalDataSet<Integer>(5);
        final ArrayList<IDataSet<Integer>> elements = new ArrayList<IDataSet<Integer>>(2);
        elements.add(ld1);
        elements.add(ld2);
        final ParallelDataSet<Integer> par = new ParallelDataSet<Integer>(elements);
        final Increment increment = new Increment();

        final IDataSet<Integer> r1 = par.blockingMap(increment);
        final Sketch sketch = new Sketch();
        final int result = r1.blockingSketch(sketch);
        Assert.assertEquals(result, 11);

        final IDataSet<Integer> r2 = r1.blockingMap(increment);
        final int result1 = r2.blockingSketch(sketch);
        Assert.assertEquals(result1, 13);
    }

    @Test
    public void parallelProgressTest() {
        int partitions = 10;
        final ArrayList<IDataSet<Integer>> outer = new ArrayList<IDataSet<Integer>>(partitions);
        for (int i = 0; i < partitions; i++) {
            final LocalDataSet<Integer> ld1 = new LocalDataSet<Integer>(4);
            final LocalDataSet<Integer> ld2 = new LocalDataSet<Integer>(5);
            final ArrayList<IDataSet<Integer>> elements = new ArrayList<IDataSet<Integer>>(2);
            elements.add(ld1);
            elements.add(ld2);
            final ParallelDataSet<Integer> par = new ParallelDataSet<Integer>(elements);
            par.setBundleInterval(0);
            outer.add(par);
        }

        final ParallelDataSet<Integer> out = new ParallelDataSet<Integer>(outer);
        out.setBundleInterval(0);
        final Increment increment = new Increment();

        Observable<PartialResult<IDataSet<Integer>>> flat = out.map(increment);
        Observer<PartialResult<IDataSet<Integer>>> obs = new
                Observer<PartialResult<IDataSet<Integer>>>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onNext(PartialResult<IDataSet<Integer>> pr) {
                        // progress should never be greater than this.
                        Assert.assertTrue(.05 >= pr.deltaDone);
                    }
                };
        flat.toBlocking().subscribe(obs);
    }

    private class Sum implements ISketch<int[], Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(@Nullable final Integer left, @Nullable final Integer right) {
            return Converters.checkNull(left) + Converters.checkNull(right);
        }

        @Override
        public Integer create(@Nullable final int[] data) {
            int sum = 0;
            for (int aData : Converters.checkNull(data)) sum += aData;
            return sum;
        }
    }

    private final int largeSize = 10 * 1024 * 1024;
    private static final int parts = 10;

    private ParallelDataSet<int[]> createLargeDataset(final boolean separateThread) {
        ArrayList<IDataSet<int[]>> l = new ArrayList<IDataSet<int[]>>(parts);
        int v = 0;
        for (int j = 0; j < parts; j++) {
            int partSize = this.largeSize / parts;
            final int[] data = new int[partSize];
            for (int i = 0; i < partSize; i++) {
                data[i] = ((v % 10) == 0) ? 0 : v;
                v++;
            }
            LocalDataSet<int[]> ld = new LocalDataSet<int[]>(data, separateThread);
            l.add(ld);
        }
        return new ParallelDataSet<int[]>(l);
    }

    @Test
    public void largeDataSetTest() {
        ParallelDataSet<int[]> ld = this.createLargeDataset(false);
        int result = ld.blockingSketch(new Sum());
        int sum = 0;
        for (int i = 0; i < this.largeSize; i++)
            sum += ((i % 10) == 0) ? 0 : i;
        Assert.assertEquals(result, sum);

        ld.setBundleInterval(100);
        result = ld.blockingSketch(new Sum());
        Assert.assertEquals(result, sum);

        ld = this.createLargeDataset(true);
        ld.setBundleInterval(100);
        result = ld.blockingSketch(new Sum());
        Assert.assertEquals(result, sum);
    }

    @Test
    public void separateThreadDataSetTest() {
        final IDataSet<int[]> ld = this.createLargeDataset(true);
        final int result = ld.blockingSketch(new Sum());
        int sum = 0;
        for (int i = 0; i < this.largeSize; i++)
            sum += ((i % 10) == 0) ? 0 : i;
        Assert.assertEquals(result, sum);
    }

    @Test
    public void unsubscriptionTest() {
        ParallelDataSet<int[]> ld = this.createLargeDataset(true);
        ld.setBundleInterval(0);  // important for this test
        Observable<PartialResult<Integer>> pr = ld.sketch(new Sum());
        TestSubscriber<PartialResult<Integer>> ts =
                new TestSubscriber<PartialResult<Integer>>() {
                    private int count = 0;

                    @Override
                    public void onNext(final PartialResult<Integer> pr) {
                        super.onNext(pr);
                        this.count++;
                        if (this.count == 3)
                            this.unsubscribe();
                    }
                };
        pr.toBlocking().subscribe(ts);

        ts.assertNotCompleted();
        ts.assertValueCount(3);
    }

    // The following are testing unsubscription
    private static final AtomicLong initialTime = new AtomicLong(0);
    private static final AtomicInteger completedLocals = new AtomicInteger(0);

    private static long getTime() {
        long time = System.currentTimeMillis();
        initialTime.compareAndSet(0, time);
        return time - initialTime.get();
    }

    static class SlowSketch implements ISketch<Integer, Integer> {
        @Override
        public Integer create(Integer data) {
            System.out.println(getTime() + " working " + data);
            int sum = 0;
            for (int i = 0; i < 10000000; i++)
                sum = (sum + i) % 43;
            System.out.println(getTime() + " ready " + data + ": " + sum);
            completedLocals.getAndIncrement();
            return 0;
        }

        @Nullable
        @Override
        public Integer zero() {
            return 0;
        }

        @Nullable
        @Override
        public Integer add(@Nullable Integer left, @Nullable Integer right) {
            return 0;
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

        HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1", 1235);
        HillviewServer server = new HillviewServer(serverAddress, ld);
        IDataSet<Integer> remote = new RemoteDataSet<Integer>(serverAddress);

        SlowSketch sketch = new SlowSketch();
        PartialResultMonoid<Integer> prm = new PartialResultMonoid<Integer>(sketch);

        for (int i = 0; i < 2; i++) {
            completedLocals.set(0);
            IDataSet<Integer> toTest = i == 0 ? ld : remote;
            Observable<PartialResult<Integer>> src = toTest.sketch(sketch).scan(prm::add);
            Observer<PartialResult<Integer>> obs = new Observer<PartialResult<Integer>>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onNext(PartialResult<Integer> i) {
                }
            };
            src = src.doOnUnsubscribe(() -> System.out.println(getTime() + " unsubscribed"));

            System.out.println(getTime() + " starting");
            Subscription sub = src.subscribe(obs);
            System.out.println(getTime() + " sleeping");
            Thread.sleep(200);
            System.out.println(getTime() + " unsubscribing");
            sub.unsubscribe();
            System.out.println(getTime() + " Sleeping some more");
            Thread.sleep(3000);
            System.out.println("Completed locals: " + completedLocals.get());

            while (!sub.isUnsubscribed())
                Thread.sleep(50);
        }

        server.shutdown();
    }

    static class SlowMap implements IMap<Integer, Integer> {
        @Override
        public Integer apply(Integer data) {
            System.out.println(getTime() + " working " + data);
            int sum = 0;
            for (int i = 0; i < 10000000; i++)
                sum = (sum + i) % 43;
            System.out.println(getTime() + " ready " + data + ": " + sum);
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
    public void unsubscriptionTest2() throws InterruptedException, IOException {
        ArrayList<IDataSet<Integer>> l = new ArrayList<IDataSet<Integer>>(100);
        for (int j = 0; j < 100; j++) {
            LocalDataSet<Integer> ld = new LocalDataSet<Integer>(j, true);
            l.add(ld);
        }
        ParallelDataSet<Integer> ld = new ParallelDataSet<Integer>(l);

        HostAndPort serverAddress = HostAndPort.fromParts("127.0.0.1", 1236);
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
            src = src.doOnUnsubscribe(() -> System.out.println(getTime() + " unsubscribed"));

            System.out.println(getTime() + " starting");
            Subscription sub = src.subscribe(obs);
            System.out.println(getTime() + " sleeping");
            Thread.sleep(200);
            System.out.println(getTime() + " unsubscribing");
            sub.unsubscribe();
            System.out.println(getTime() + " Sleeping some more");
            Thread.sleep(5000);
            System.out.println("Completed locals: " + completedLocals.get());

            while (!sub.isUnsubscribed())
                Thread.sleep(50);
        }
        server.shutdown();
    }
}
