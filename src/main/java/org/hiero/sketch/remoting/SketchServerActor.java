package org.hiero.sketch.remoting;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.serialization.Serialization;
import org.hiero.sketch.dataset.api.*;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A simple Sketch Server realized as an actor. It
 * accepts a LocalDataSet in its constructor that it executes
 * incoming Map and Sketch operations against.
 * @param <T> Type of IDataSet backing the actor
 */
public class SketchServerActor<T> extends AbstractActor {

    private static final String SERVER_ACTOR_NAME = "ServerActor";
    private static final AtomicInteger nodeId = new AtomicInteger(0);

    private final IDataSet<T> dataSet;
    private final ConcurrentHashMap<UUID, Subscription> operationToObservable
            = new ConcurrentHashMap<>();


    @SuppressWarnings("unchecked")
    public SketchServerActor(final IDataSet<T> dataSet) {
        this.dataSet = dataSet;

        receive(
            ReceiveBuilder

            // Handle IMap messages and respond
            // to the sender with a success message
            // XXX: Need the S in IMap<T,S> to specify the type of the observable correctly
            .match(MapOperation.class, mapOp -> {
                Observable<PartialResult<IDataSet>> observable = this.dataSet.map(mapOp.mapper);
                Subscription sub =
                    observable.subscribe(new MapResponderSubscriber<PartialResult<IDataSet>>
                                         (mapOp.id, sender()));
                this.operationToObservable.put(mapOp.id, sub);
            })

            // Handle ISketch messages and respond to the sender
            // with the result
            // XXX: Need the R in ISketch<T,R> to specify the type of the observable correctly
            .match(SketchOperation.class, sketchOp -> {
                Observable<PartialResult> observable = this.dataSet.sketch(sketchOp.sketch);
                Subscription sub = observable.subscribe(new ResponderSubscriber<PartialResult>
                                                        (sketchOp.id, sender()));
                this.operationToObservable.put(sketchOp.id, sub);
            })

            // Zips are executed as leftDataSet.zip(rightDataSet). The recipient
            // of a ZipOperation is the actor pointing to the leftDataSet. We therefore
            // collect the leftDataSet reference here, and send a ZipExecute message to the
            // rightDataSet which is what will execute the Zip and stream the
            // result back to the original sender.
            .match(ZipOperation.class,
                zipOp -> zipOp.remoteActor.tell(new ZipExecute(zipOp, dataSet, sender()), self()))

            // Triggered by an incoming ZipOperation above.
            .match(ZipExecute.class, zipExec -> {
                Observable<PartialResult<IDataSet<Pair>>> observable =
                        zipExec.dataSet.zip(this.dataSet);
                Subscription sub = observable.subscribe(new ZipResponderSubscriber<PartialResult>
                        (zipExec.zipOp.id, zipExec.clientActor));
                this.operationToObservable.put(zipExec.zipOp.id, sub);
            })

            // Unsubscribe messages
            .match(UnsubscribeOperation.class, unsubscribeOp -> {
                if (this.operationToObservable.containsKey(unsubscribeOp.id)) {
                    Subscription sub = this.operationToObservable.remove(unsubscribeOp.id);
                    sub.unsubscribe();
                }
            })

            // Default
            .matchAny(
                this::unhandled
            )
            .build()
        );
    }

    private class MapResponderSubscriber<R> extends ResponderSubscriber<R> {
        private final PartialResultMonoid resultMonoid = new PRDataSetMonoid();
        private PartialResult result = this.resultMonoid.zero();

        private MapResponderSubscriber(final UUID id, final ActorRef sender) {
            super(id, sender);
        }

        @Override
        public void onCompleted() {
            if (!isUnsubscribed()) {
                final String newActor = SERVER_ACTOR_NAME + nodeId.incrementAndGet();
                final ActorRef actorRef = context().actorOf(Props.create(SketchServerActor.class,
                                               this.result.deltaValue), newActor);
                final OperationResponse<String> response =
                        new OperationResponse<String>(Serialization.serializedActorPath(actorRef), this.id,
                                                      OperationResponse.Type.NewRemoteDataSet);
                this.sender.tell(response, self());
            }
        }

        @Override
        public void onNext(final R r) {
            if (!isUnsubscribed()) {
                final PartialResult pr = (PartialResult) r;
                this.result = this.resultMonoid.add(this.result, pr);
                super.onNext(r);
            }
        }
    }

    private class ZipResponderSubscriber<R> extends ResponderSubscriber<R> {
        private final PartialResultMonoid resultMonoid = new PRDataSetMonoid();
        private final PartialResult result = this.resultMonoid.zero();

        private ZipResponderSubscriber(final UUID id, final ActorRef sender) {
            super(id, sender);
        }

        @Override
        public void onCompleted() {
            if (!isUnsubscribed()) {
                final String newActor = SERVER_ACTOR_NAME + nodeId.incrementAndGet();
                final ActorRef actorRef = context().actorOf(Props.create(SketchServerActor.class,
                        this.result.deltaValue), newActor);
                final OperationResponse<String> response =
                        new OperationResponse<String>(Serialization.serializedActorPath(actorRef), this.id,
                                OperationResponse.Type.NewRemoteDataSet);
                this.sender.tell(response, self());
            }
        }
    }

    /**
     * Generic subscriber, used to wrap results and send them back to the client
     */
    private class ResponderSubscriber<R> extends Subscriber<R> {
        final UUID id;
        final ActorRef sender;

        private ResponderSubscriber(final UUID id, final ActorRef sender) {
            this.id = id;
            this.sender = sender;
        }

        @Override
        public void onCompleted() {
            if (!isUnsubscribed()) {
                final OperationResponse<Integer> response = new OperationResponse<Integer>(0, this.id,
                        OperationResponse.Type.OnCompletion);
                this.sender.tell(response, self());
                SketchServerActor.this.operationToObservable.remove(this.id);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            if (!isUnsubscribed()) {
                final OperationResponse<Throwable> response =
                        new OperationResponse<Throwable>(throwable, this.id,
                                OperationResponse.Type.OnError);
                this.sender.tell(response, self());
                SketchServerActor.this.operationToObservable.remove(this.id);
            }
        }

        @Override
        public void onNext(final R r) {
            if (!isUnsubscribed()) {
                final OperationResponse<R> response = new OperationResponse<R>(r, this.id,
                        OperationResponse.Type.OnNext);
                this.sender.tell(response, self());
            }
        }
    }
}