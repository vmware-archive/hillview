package org.hiero.sketch.remoting;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.PartialResult;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A simple Sketch Server realized as an actor. It
 * accepts a LocalDataSet in its constructor that it executes
 * incoming Map and Sketch operations against.
 * @param <T> Type of IDataSet backing the actor
 */
public class SketchServerActor<T> extends AbstractActor {

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
                    observable.subscribe(new ResponderSubscriber<PartialResult<IDataSet>>
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

    /**
     * Generic subscriber, used to wrap results and send them back to the client
     */
    private class ResponderSubscriber<R> extends Subscriber<R> {
        private final UUID id;
        private final ActorRef sender;

        private ResponderSubscriber(final UUID id, final ActorRef sender) {
            this.id = id;
            this.sender = sender;
        }

        @Override
        public void onCompleted() {
            final OperationResponse<Integer> response = new OperationResponse<Integer>(0, this.id,
                    OperationResponse.Type.OnCompletion);
            this.sender.tell(response, self());
            SketchServerActor.this.operationToObservable.remove(this.id);
        }

        @Override
        public void onError(final Throwable throwable) {
            final OperationResponse<Throwable> response =
                    new OperationResponse<Throwable>(throwable, this.id,
                            OperationResponse.Type.OnError);
            this.sender.tell(response, self());
            SketchServerActor.this.operationToObservable.remove(this.id);
        }

        @Override
        public void onNext(final R r) {
            final OperationResponse<R> response = new OperationResponse<R>(r, this.id,
                    OperationResponse.Type.OnNext);
            this.sender.tell(response, self());
        }
    }
}