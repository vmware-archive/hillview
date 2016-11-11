package org.hiero.sketch.remoting;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.hiero.sketch.dataset.RemoteDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.PartialResult;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This actor wires a local observable pertaining to a map or sketch
 * operation with its remote execution. Submitting a {Map,Sketch}Operation
 * to this actor returns an observable that streams values in accordance
 * to its execution on the remote node.
 */
public class SketchClientActor extends AbstractActor {

    private final ConcurrentHashMap<UUID, PublishSubject> operationToObservable
            = new ConcurrentHashMap<>();
    private static final AtomicInteger counter = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public SketchClientActor(final ActorSelection remoteActor) {
        receive(
            ReceiveBuilder

            // All responses from the remote node is handled here.
            .match(OperationResponse.class,
                response -> {
                    if (this.operationToObservable.containsKey(response.id)) {
                        final PublishSubject subject = this.operationToObservable.get(response.id);
                        switch (response.type) {
                            case OnCompletion:
                                subject.onCompleted();
                                this.operationToObservable.remove(response.id);
                                break;
                            case OnError:
                                subject.onError((Throwable) response.result);
                                this.operationToObservable.remove(response.id);
                                break;
                            case OnNext:
                                subject.onNext(response.result);
                                break;
                            case NewDataSet:
                                final ActorSelection newRemote =
                                        context().actorSelection("akka.tcp://SketchApplication@127.0.0.1:2554/user/ServerActor/"
                                        + response.result);
                                final ActorRef newClientActor = context().actorOf(Props.create(SketchClientActor.class, newRemote),
                                        "ClientActor" + counter.incrementAndGet());
                                IDataSet ids = new RemoteDataSet(newClientActor);
                                subject.onNext(new PartialResult<IDataSet>(0.0, ids));
                                subject.onCompleted();
                                break;
                        }
                    }
                    else {
                        System.err.println("Received response for " +
                                           "ID we are not tracking" + response.id);
                    }
                }
            )

            // Map Operation sender. It creates a PublishSubject
            // for each MapOperation we send, and rejects
            // duplicate attempts to resend the same MapOperation.
            .match(MapOperation.class,
                mapOp -> {
                    if (!this.operationToObservable.containsKey(mapOp.id)) {
                        PublishSubject subj = PublishSubject.create();
                        Observable obs = subj.doOnSubscribe(() -> {
                            // Defer sending the remote node the request until
                            // we actually subscribe, but do so only once
                            if (this.operationToObservable.putIfAbsent(mapOp.id, subj) == null) {
                                remoteActor.tell(mapOp, self());
                            }
                        }).doOnUnsubscribe(() -> {
                            // It's fine if the below code gets invoked twice, the
                            // remote end is idempotent
                            if (this.operationToObservable.containsKey(mapOp.id)) {
                                sender().tell(new UnsubscribeOperation(mapOp.id), self());
                                this.operationToObservable.remove(mapOp.id);
                            }
                        });
                        sender().tell(obs, self());
                    }
                }
            )

            // Sketch Operation sender. It creates a PublishSubject
            // for each SketchOperation we send, and rejects
            // duplicate attempts to resend the same SketchOperation.
            .match(SketchOperation.class,
                sketchOp -> {
                    if (!this.operationToObservable.containsKey(sketchOp.id)) {
                        PublishSubject subj = PublishSubject.create();
                        Observable obs = subj.doOnSubscribe(() -> {
                            // Defer sending the remote node the request until
                            // we actually subscribe, but do so only once
                            if (this.operationToObservable.putIfAbsent(sketchOp.id, subj) == null) {
                                remoteActor.tell(sketchOp, self());
                            }
                        }).doOnUnsubscribe(() -> {
                            // It's fine if the below code gets invoked twice, the
                            // remote end is idempotent
                            if (this.operationToObservable.containsKey(sketchOp.id)) {
                                PublishSubject ps = this.operationToObservable.remove(sketchOp.id);
                                sender().tell(new UnsubscribeOperation(sketchOp.id), self());
                            }
                        });
                        sender().tell(obs, self());
                    }
                }
            )

            // Everything else
            .matchAny(
                this::unhandled
            )
            .build()
        );
    }
}