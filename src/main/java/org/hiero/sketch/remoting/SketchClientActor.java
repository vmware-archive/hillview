package org.hiero.sketch.remoting;

import akka.actor.*;
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

    private static final String CLIENT_ACTOR_NAME = "ClientActor";
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final ConcurrentHashMap<UUID, PublishSubject> operationToObservable
            = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public SketchClientActor(final ActorRef remoteActor) {

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
                            case NewRemoteDataSet:
                                final ActorRef newRemote =
                                        context().actorFor((String) response.result);
                                final ActorRef newClientActor = context().actorOf(Props.create(SketchClientActor.class, newRemote),
                                        CLIENT_ACTOR_NAME + counter.incrementAndGet());
                                IDataSet ids = new RemoteDataSet(newClientActor, newRemote);
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

            // Remote Operation sender. It creates a PublishSubject
            // for each RemoteOperation we send, and rejects
            // duplicate attempts to resend the same Operation.
            .match(RemoteOperation.class,
                    remoteOp -> sendOperation(remoteOp, remoteActor, sender())
            )

            // Everything else
            .matchAny(
                this::unhandled
            )
            .build()
        );
    }

    private <T extends RemoteOperation> void sendOperation(final T operation,
                                                           final ActorRef remoteActor,
                                                           final ActorRef sender) {
        if (!this.operationToObservable.containsKey(operation.id)) {
            final PublishSubject subj = PublishSubject.create();
            final Observable obs = subj.doOnSubscribe(() -> {
                // Defer sending the remote node the request until
                // we actually subscribe, but do so only once
                if (this.operationToObservable.putIfAbsent(operation.id, subj) == null) {
                    remoteActor.tell(operation, self());
                }
            }).doOnUnsubscribe(() -> {
                // It's fine if the below code gets invoked twice, the
                // remote end is idempotent
                if (this.operationToObservable.containsKey(operation.id)) {
                    final PublishSubject ps = this.operationToObservable.remove(operation.id);
                    sender().tell(new UnsubscribeOperation(operation.id), self());
                }
            });
            sender.tell(obs, self());
        }
    }
}