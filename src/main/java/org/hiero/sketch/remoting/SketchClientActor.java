package org.hiero.sketch.remoting;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.util.Timeout;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.RemoteDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.PartialResult;
import rx.Observable;
import rx.subjects.PublishSubject;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

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
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private static final Timeout RESOLVE_TIMEOUT =
            new Timeout(Duration.create(1000, "milliseconds"));
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
                                log.debug("OnCompletion {} {}", response.id, response.result);
                                subject.onCompleted();
                                this.operationToObservable.remove(response.id);
                                break;
                            case OnError:
                                log.debug("OnError {} {}", response.id, response.result);
                                subject.onError((Throwable) response.result);
                                this.operationToObservable.remove(response.id);
                                break;
                            case OnNext:
                                log.debug("OnNext {} {}", response.id, response.result);
                                subject.onNext(response.result);
                                break;
                            case NewRemoteDataSet:
                                final ActorRef newRemote = Await.result(context().actorSelection(
                                        (String) response.result).resolveOne(RESOLVE_TIMEOUT),
                                        RESOLVE_TIMEOUT.duration());
                                final ActorRef newClientActor =
                                        context().actorOf(Props.create(SketchClientActor.class,
                                                                       newRemote),
                                        CLIENT_ACTOR_NAME + counter.incrementAndGet());
                                IDataSet ids = new RemoteDataSet(newClientActor, newRemote);
                                subject.onNext(new PartialResult<IDataSet>(1.0, ids));
                                subject.onCompleted();
                                break;
                        }
                    }
                    else {
                        // This is to handle a race condition where the local unsubscribe
                        // is executed, and we receive a stream of in flight
                        // on{Next, Completion, Error} events from the remote server, before
                        // the remote unsubscribe is executed.
                        log.error("Received response for ID we are not tracking: {}" + response.id);
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

    private <T extends RemoteOperation> void sendOperation(@NonNull final T operation,
                                                           @NonNull final ActorRef remoteActor,
                                                           @NonNull final ActorRef sender) {
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