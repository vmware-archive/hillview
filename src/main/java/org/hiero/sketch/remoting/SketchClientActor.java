package org.hiero.sketch.remoting;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import akka.japi.pf.ReceiveBuilder;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This actor wires a local observable pertaining to a map or sketch
 * operation with its remote execution. Submitting a {Map,Sketch}Operation
 * to this actor returns an observable that streams values in accordance
 * to its execution on the remote node.
 */
public class SketchClientActor extends AbstractActor {

    private final ConcurrentHashMap<UUID, PublishSubject> operationToObservable
            = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public SketchClientActor(final ActorSelection remoteActor) {
        receive(
            ReceiveBuilder

            // All responses from the remote node is handled here.
            .match(OperationResponse.class,
                response -> {
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
                                sender().tell(new UnsubscribeOperation(sketchOp.id), self());
                                this.operationToObservable.remove(sketchOp.id);
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