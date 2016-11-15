package org.hiero.sketch;

import akka.actor.*;
import akka.util.Timeout;
ßimport com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.TestCase;
import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.RemoteDataSet;
import org.hiero.sketch.dataset.api.*;
import org.hiero.sketch.remoting.SketchOperation;
import org.hiero.sketch.remoting.SketchClientActor;
import org.hiero.sketch.remoting.SketchServerActor;
import static akka.pattern.Patterns.ask;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;

import rx.Observable;
import rx.Subscriber;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Remoting tests for Akka.
 */
public class AkkaTest {
    private static ActorRef clientActor;
    private static ActorRef remoteActor;

    private class IncrementMap implements IMap<int[], int[]> {
        @Override
        public Observable<PartialResult<int[]>> apply(final int[] data) {
            if (data.length == 0) {
                throw new RuntimeException("Cannot apply map against empty data");
            }

            final int[] dataNew = new int[data.length];
            for (int i = 0; i < data.length; i++) {
                dataNew[i] = data[i] + 1;
            }

            return Observable.just(new PartialResult<int[]>(1.0, dataNew));
        }
    }

    private class SumSketch implements ISketch<int[], Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(final Integer left, final Integer right) {
            return left + right;
        }

        @Override
        public Observable<PartialResult<Integer>> create(final int[] data) {
            final int parts = 10;
            return Observable.range(0, parts).map(index -> {
                final int partSize = data.length / parts;
                final int left = partSize * index;
                final int right = (index == (parts - 1)) ? data.length : (left + partSize);
                int sum1 = 0;
                for (int i = left; i < right; i++) {
                    sum1 += data[i];
                }
                return new PartialResult<Integer>(1.0 / parts, sum1);
            });
        }
    }

    /*
     * Create separate server and client actor systems to test remoting.
     */
    @BeforeClass
    public static void initialize() {
        // Server
        final String serverConfFileUrl = ClassLoader.getSystemResource("test-server.conf").getFile();
        final Config serverConfig = ConfigFactory.parseFile(new File(serverConfFileUrl));
        final ActorSystem serverActorSystem = ActorSystem.create("SketchApplication", serverConfig);
        assertNotNull(serverActorSystem);

        // Create a dataset
        final int size = 10000;
        final int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
        final LocalDataSet<int[]> lds = new LocalDataSet<>(data);
        remoteActor = serverActorSystem.actorOf(Props.create(SketchServerActor.class, lds),
                                                "ServerActor");

        // Client
        final String clientConfFileUrl = ClassLoader.getSystemResource("client.conf").getFile();
        final Config clientConfig = ConfigFactory.parseFile(new File(clientConfFileUrl));
        final ActorSystem clientActorSystem = ActorSystem.create("SketchApplication", clientConfig);
        final ActorRef remoteActor = clientActorSystem.actorFor(
                "akka.tcp://SketchApplication@127.0.0.1:2554/user/ServerActor");
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
            obs.subscribe();
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
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(clientActor, remoteActor);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue;
        assertNotNull(remoteIdsNew);
        final int result = remoteIdsNew.sketch(new SumSketch())
                                       .map(e -> e.deltaValue)
                                       .reduce((x, y) -> x + y)
                                       .toBlocking()
                                       .last();
        assertEquals(50005000, result);
    }

    @Test
    public void testMapSketchThroughClientUnsubscribe() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(clientActor, remoteActor);
        final IDataSet<int[]> remoteIdsNew = remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue;
        assertNotNull(remoteIdsNew);
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        final Observable<PartialResult<Integer>> resultObs = remoteIds.sketch(new SumSketch());
        final AtomicInteger counter = new AtomicInteger(0);
        resultObs.subscribe(new Subscriber<PartialResult<Integer>>() {
            private double done = 0.0;

            @Override
            public void onCompleted() {
                fail("Unreachable");
            }

            @Override
            public void onError(final Throwable throwable) {
                fail("Unreachable");
            }

            @Override
            public void onNext(final PartialResult<Integer> pr) {
                this.done += pr.deltaDone;
                final int count = counter.incrementAndGet();
                countDownLatch.countDown();
                if (count == 3) {
                    this.unsubscribe();
                }
                else {
                    TestCase.assertEquals(this.done, 0.1 * count);
                }
            }
        });

        try {
            countDownLatch.await();
        } catch (final InterruptedException e) {
            fail("Should not happen");
        }

        assertEquals(3, counter.get());
    }


    @Test
    public void testZip() {
        final IDataSet<int[]> remoteIds = new RemoteDataSet<int[]>(clientActor, remoteActor);
        final IDataSet<int[]> remoteIdsLeft = remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue;
        final IDataSet<int[]> remoteIdsRight = remoteIds.map(new IncrementMap()).toBlocking().last().deltaValue;
        final PartialResult<IDataSet<Pair<int[], int[]>>> last
                = remoteIdsLeft.zip(remoteIdsRight).toBlocking().last();
        assertNotNull(last);
    }
}