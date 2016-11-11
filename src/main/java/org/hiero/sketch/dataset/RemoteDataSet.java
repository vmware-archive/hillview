package org.hiero.sketch.dataset;

import akka.actor.ActorRef;
import akka.util.Timeout;
import org.hiero.sketch.dataset.api.*;
import org.hiero.sketch.remoting.MapOperation;
import org.hiero.sketch.remoting.SketchOperation;
import rx.Observable;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;

/**
 * An IDataSet that is a proxy for a DataSet on a remote machine.
 */
public class RemoteDataSet<T> implements IDataSet<T> {
    private final ActorRef clientActor;


    public RemoteDataSet(final ActorRef clientActor) {
        this.clientActor = clientActor;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        final Timeout timeout = new Timeout(Duration.create(5, "seconds"));
        final MapOperation mapOp = new MapOperation(mapper);
        final Future<Object> future = ask(this.clientActor, mapOp, 1000);
        Observable<PartialResult<IDataSet<S>>> obs = null;
        try {
            obs = (Observable<PartialResult<IDataSet<S>>>) Await.result(future, timeout.duration());
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return obs;
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        final Timeout timeout = new Timeout(Duration.create(5, "seconds"));
        final SketchOperation sketchOp = new SketchOperation(sketch);
        final Future<Object> future = ask(this.clientActor, sketchOp, 1000);
        Observable<PartialResult<R>> obs = null;
        try {
            obs = (Observable<PartialResult<R>>) Await.result(future, timeout.duration());
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return obs;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        return null;
    }
}