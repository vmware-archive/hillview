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
 *
 */

package org.hiero.sketch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.RemoteDataSet;
import org.hiero.sketch.dataset.api.*;
import org.hiero.sketch.remoting.SketchClientActor;
import org.hiero.sketch.remoting.SketchOperation;
import org.hiero.sketch.remoting.SketchServerActor;
import org.hiero.utils.Converters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;

import static akka.pattern.Patterns.ask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Remoting tests for Akka.
 */
public class RemotingTest {
    @Nullable
    private static ActorSystem clientActorSystem;
    @Nullable
    private static ActorSystem serverActorSystem;
    @Nullable
    private static ActorRef clientActor;
    @Nullable
    private static ActorRef remoteActor;

    private class IncrementMap implements IMap<int[], int[]> {
        @Override
        public int[] apply(final int[] data) {
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

    private class SumSketch implements ISketch<int[], Integer> {
        @Override @Nullable
        public Integer zero() {
            return 0;
        }

        @Override @Nullable
        public Integer add(@Nullable final Integer left, @Nullable final Integer right) {
            return Converters.checkNull(left) + Converters.checkNull(right);
        }

        @Override
        public Integer create(final int[] data) {
            int sum = 0;
            for (int d : data) sum += d;
            return sum;
        }
    }

    private class ErrorSumSketch implements ISketch<int[], Integer> {
        @Override @Nullable
        public Integer zero() {
            return 0;
        }

        @Override @Nullable
        public Integer add(@Nullable final Integer left, @Nullable final Integer right) {
            return Converters.checkNull(left) + Converters.checkNull(right);
        }

        @Override
        public Integer create(final int[] data) {
            throw new RuntimeException("ErrorSumSketch");
        }
    }

    /*
     * Create separate server and client actor systems to test remoting.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        // Server
        final Timeout timeout = new Timeout(Duration.create(1000, "milliseconds"));
        final String serverConfFileUrl = ClassLoader.getSystemResource("test-server.conf").getFile();
        final Config serverConfig = ConfigFactory.parseFile(new File(serverConfFileUrl));
        serverActorSystem = ActorSystem.create("SketchApplication", serverConfig);
        assertNotNull(serverActorSystem);

        // Create a dataset
        final int parts = 10;
        final int size = 1000;
        ArrayList<IDataSet<int[]>> al = new ArrayList<IDataSet<int[]>>(10);
        for (int i=0; i < parts; i++) {
            final int[] data = new int[size];
            for (int j = 0; j < size; j++)
                data[j] = (i * size) + j;
            LocalDataSet<int[]> lds = new LocalDataSet<>(data);
            al.add(lds);
        }
        ParallelDataSet<int[]> pds = new ParallelDataSet<int[]>(al);
        pds.setBundleInterval(0);
        remoteActor = serverActorSystem.actorOf(Props.create(SketchServerActor.class, pds),
                                                "ServerActor");

        // Client
        final String clientConfFileUrl = ClassLoader.getSystemResource("client.conf").getFile();
        final Config clientConfig = ConfigFactory.parseFile(new File(clientConfFileUrl));
        clientActorSystem = ActorSystem.create("SketchApplication", clientConfig);
        final ActorRef remoteActor = Await.result(clientActorSystem.actorSelection(
                "akka.tcp://SketchApplication@127.0.0.1:2554/user/ServerActor").resolveOne(timeout),
                timeout.duration());
        clientActor = clientActorSystem.actorOf(Props.create(SketchClientActor.class, remoteActor), "ClientActor");
        assertNotNull(clientActor);
    }

    @Test
    public void testSketchThroughClient() {
        final int timeoutDuration = 1000;
        final Timeout timeout = new Timeout(Duration.create(timeoutDuration, "milliseconds"));
        final SketchOperation<int[], Integer> sketchOp = new SketchOperation<int[], Integer>(new SumSketch());
        final Future<Object> future = ask(clientActor, sketchOp, timeoutDuration);
        try {
            @SuppressWarnings("unchecked")
            final Observable<PartialResult<Integer>> obs =
                    (Observable<PartialResult<Integer>>) Await.result(future, timeout.duration());
            final int result = obs.map(e -> e.deltaValue)
                                   .reduce((x, y) -> x + y)
                                   .toBlocking()
                                   .last();
            assertEquals(49995000, result);
        } catch (final Exception e) {
            fail("Should not have thrown exception");
        }
    }

    @Test
    public void testMapSketchThroughClient() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(
                Converters.checkNull(clientActor), Converters.checkNull(remoteActor));
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                                                      .filter(p -> p.deltaValue != null)
                                                      .toBlocking()
                                                      .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final int result = remoteIdsNew.sketch(new SumSketch())
                                       .map(e -> e.deltaValue)
                                       .reduce((x, y) -> x + y)
                                       .toBlocking()
                                       .last();
        assertEquals(50005000, result);
    }


    //@Test: TODO: re-enable this test
    public void testMapSketchThroughClientWithError() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(
                Converters.checkNull(clientActor), Converters.checkNull(remoteActor));
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap())
                                                      .toBlocking()
                                                      .last().deltaValue;
        assertNotNull(remoteIdsNew);
        final Observable<PartialResult<Integer>> resultObs =
                remoteIdsNew.sketch(new ErrorSumSketch());
        TestSubscriber<PartialResult<Integer>> ts = new TestSubscriber<PartialResult<Integer>>();
        resultObs.toBlocking().subscribe(ts);
        ts.assertError(RuntimeException.class);
    }

    @Test
    public void testMapSketchThroughClientUnsubscribe() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(
                Converters.checkNull(clientActor), Converters.checkNull(remoteActor));
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue;
        assertNotNull(remoteIdsNew);

        final Observable<PartialResult<Integer>> resultObs = remoteIds.sketch(new SumSketch());
        TestSubscriber<PartialResult<Integer>> ts =
                new TestSubscriber<PartialResult<Integer>>() {
                    private int counter = 0;

                    @Override
                    public void onNext(final PartialResult<Integer> pr) {
                        this.counter++;
                        super.onNext(pr);
                        if (this.counter == 3)
                            this.unsubscribe();
                    }
                };

        resultObs.toBlocking().subscribe(ts);
        ts.assertValueCount(3);
        ts.assertNotCompleted();
    }

    @Test
    public void testZip() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(
                Converters.checkNull(clientActor), Converters.checkNull(remoteActor));
        final IDataSet<int[]> remoteIdsLeft = Converters.checkNull(
                remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue);
        final IDataSet<int[]> remoteIdsRight = Converters.checkNull(
                remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue);
        final PartialResult<IDataSet<Pair<int[], int[]>>> last
                = Converters.checkNull(remoteIdsLeft.zip(remoteIdsRight)).toBlocking().last();
        assertNotNull(last);
        assertEquals(last.deltaDone, 1.0, 0.001);
    }

    @AfterClass
    public static void shutdown() {
        if (clientActorSystem != null)
            clientActorSystem.terminate();
        if (serverActorSystem != null)
            serverActorSystem.terminate();
    }
}