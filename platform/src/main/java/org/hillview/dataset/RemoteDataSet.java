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

package org.hillview.dataset;

import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.hillview.dataset.api.*;
import org.hillview.pb.Ack;
import org.hillview.pb.Command;
import org.hillview.pb.HillviewServerGrpc;
import org.hillview.pb.PartialResponse;
import org.hillview.dataset.remoting.*;
import org.hillview.utils.*;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An IDataSet that is a proxy for a DataSet on a remote machine. The remote IDataSet
 * is pointed to by (serverEndpoint, remoteHandle). Any RemoteDataSet instantiated
 * with a wrong value for either entry of the tuple will result in an exception.
 */
public class RemoteDataSet<T> extends BaseDataSet<T> {
    private final static int TIMEOUT = 60000 * 10;  // TODO: import via config file
    private final int remoteHandle;
    private final HostAndPort serverEndpoint;
    private final HillviewServerGrpc.HillviewServerStub stub;
    /*
     * To avoid epoll CPU utilization problems, we could use PollSelectorProvider().
     * See: https://github.com/netty/netty/issues/327
     */
    private static final EventLoopGroup workerElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("rds-shared-worker"));
    /*
     * The high priority for the executor is needed to handle unsubscription
     * requests with high priority.
     */
    private static final ExecutorService executorService =
            ExecutorUtils.newNamedThreadPool("rds-shared-executor", 5, Thread.MAX_PRIORITY);
    /**
     * When the service is deployed with a single initial dataset this is the index used.
     */
    public static final int defaultDatasetIndex = -1;

    /**
     * Creates a parallel dataset with one representative for each machine in the
     * specified cluster.
     * @param index   Index of dataset on remote machine.  Must be a negative number.
     */
    public static IDataSet<Empty> createCluster(final HostList description, int index) {
        final int numServers = description.getServerList().size();
        if (numServers <= 0) {
            throw new IllegalArgumentException("ClusterDescription must contain one or more servers");
        }
        HillviewLogger.instance.info("Creating parallel dataset");
        final ArrayList<IDataSet<Empty>> emptyDatasets = new ArrayList<IDataSet<Empty>>(numServers);
        description.getServerList().forEach(server -> emptyDatasets.add(
                new RemoteDataSet<Empty>(server, index)));
        return new ParallelDataSet<Empty>(emptyDatasets);
    }

    public RemoteDataSet(final HostAndPort serverEndpoint) {
        this(serverEndpoint, -1);
    }

    public RemoteDataSet(final HostAndPort serverEndpoint, final int remoteHandle) {
        this.serverEndpoint = serverEndpoint;
        this.remoteHandle = remoteHandle;
        this.stub = HillviewServerGrpc.newStub(NettyChannelBuilder
                .forAddress(serverEndpoint.getHost(), serverEndpoint.getPort())
                .maxInboundMessageSize(HillviewServer.MAX_MESSAGE_SIZE)
                .executor(executorService)
                .eventLoopGroup(workerElg)
                .usePlaintext()   // channel is unencrypted.
                .build());
    }

    private static <T> SerializedSubject<T, T> createSerializedSubject() {
        return PublishSubject.<T>create().toSerialized();
    }

    public String toString() {
        return this.serverEndpoint.toString();
    }

    /**
     * Convenience class that is used to create a Command and the observers
     * waiting for its results.
     * @param <S> Type of result produced by command.
     */
    class CommandWrapper<S> {
        final Command command;
        final UUID operationId;
        final SerializedSubject<PartialResult<S>, PartialResult<S>> subject;
        @Nullable
        StreamObserver<PartialResponse> responseObserver;

        CommandWrapper(RemoteOperation operation) {
            byte[] serializedOp = SerializationUtils.serialize(operation);
            this.operationId = UUID.randomUUID();
            this.command = Command.newBuilder()
                    .setIdsIndex(RemoteDataSet.this.remoteHandle)
                    .setSerializedOp(ByteString.copyFrom(serializedOp))
                    .setHighId(operationId.getMostSignificantBits())
                    .setLowId(operationId.getLeastSignificantBits())
                    .build();
            this.subject = RemoteDataSet.createSerializedSubject();
            this.responseObserver = null;
        }
    }

    /**
     * A CommandWrapper that returns an IDataSet[S].
     * @param <S>  type of data in returned dataset.
     */
    class DatasetCommandWrapper<S> extends CommandWrapper<IDataSet<S>> {
        DatasetCommandWrapper(RemoteOperation operation) {
            super(operation);
            this.responseObserver = new NewDataSetObserver<S>(this.subject);
        }
    }

    /**
     * Map operations on a RemoteDataSet result in only one onNext
     * invocation that will return the final IDataSet.
     */
    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        final MapOperation<T, S> mapOp = new MapOperation<T, S>(mapper);
        DatasetCommandWrapper<S> wrap = new DatasetCommandWrapper<S>(mapOp);
        return wrap.subject.unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .map(wrap.command, wrap.responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    /**
     * Prune operations on a RemoteDataSet result in only one onNext
     * invocation that will return the final IDataSet.
     */
    @Override
    public Observable<PartialResult<IDataSet<T>>> prune(final IMap<T, Boolean> isEmpty) {
        final PruneOperation<T> pruneOp = new PruneOperation<T>(isEmpty);
        DatasetCommandWrapper<T> wrap = new DatasetCommandWrapper<T>(pruneOp);
        return wrap.subject.unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .prune(wrap.command, wrap.responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper) {
        final FlatMapOperation<T, S> mapOp = new FlatMapOperation<T, S>(mapper);
        DatasetCommandWrapper<S> wrap = new DatasetCommandWrapper<S>(mapOp);
        return wrap.subject.unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .flatMap(wrap.command, wrap.responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    /**
     * Sketch operation that streams partial results from the server to the caller.
     */
    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        final SketchOperation<T, R> sketchOp = new SketchOperation<T, R>(sketch);
        CommandWrapper<R> wrap = new CommandWrapper<R>(sketchOp);
        StreamObserver<PartialResponse> responseObserver = new SketchObserver<R>(wrap.subject);
        return wrap.subject
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .sketch(wrap.command, responseObserver))
                .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    /**
     * Zip operation on two IDataSet objects that need to reside on the same remote server.
     */
    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(final IDataSet<S> other) {
        if (!(other instanceof RemoteDataSet<?>)) {
            throw new RuntimeException("Unexpected type in Zip " + other);
        }
        final RemoteDataSet<S> rds = (RemoteDataSet<S>) other;

        // zip commands are not valid if the RemoteDataSet instances point to different
        // actor systems or different nodes.
        final HostAndPort leftAddress = this.serverEndpoint;
        final HostAndPort rightAddress = rds.serverEndpoint;
        if (!leftAddress.equals(rightAddress)) {
            throw new RuntimeException("Zip command invalid for RemoteDataSets " +
                    "across different servers | left: " + leftAddress + ", right:" + rightAddress);
        }

        final ZipOperation zip = new ZipOperation(rds.remoteHandle);
        DatasetCommandWrapper<Pair<T, S>> wrap = new DatasetCommandWrapper<Pair<T, S>>(zip);
        return wrap.subject.unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .zip(wrap.command, wrap.responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    @Override
    public Observable<PartialResult<ControlMessage.StatusList>> manage(ControlMessage message) {
        final ManageOperation manageOp = new ManageOperation(message);
        CommandWrapper<ControlMessage.StatusList> wrap = new CommandWrapper<ControlMessage.StatusList>(manageOp);
        final StreamObserver<PartialResponse> responseObserver =
                new ManageObserver(wrap.subject, message, this);
        return wrap.subject.unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                        .manage(wrap.command, responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(wrap.operationId));
    }

    /**
     * Unsubscribes an operation. This method is safe to invoke multiple times because the
     * logic on the remote end is idempotent.
     */
    private void unsubscribe(final UUID id) {
        HillviewLogger.instance.info("Unsubscribe called", "{0}", id);
        final UnsubscribeOperation op = new UnsubscribeOperation(id);
        final byte[] serializedOp = SerializationUtils.serialize(op);
        final Command command = Command.newBuilder()
                                       .setIdsIndex(this.remoteHandle)
                                       .setSerializedOp(ByteString.copyFrom(serializedOp))
                                       .setHighId(id.getMostSignificantBits())
                                       .setLowId(id.getLeastSignificantBits())
                                       .build();
        this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                 .unsubscribe(command, new StreamObserver<Ack>() {
            @Override
            public void onNext(final Ack ack) {}

            @Override
            public void onError(final Throwable throwable) {}

            @Override
            public void onCompleted() {}
        });
    }

    /**
     * A StreamObserver that transfers incoming onNext, onError and onCompleted invocations
     * from a gRPC streaming call to that of a publish subject.
     */
    private abstract class OperationObserver<S> implements StreamObserver<PartialResponse> {
        final SerializedSubject<S, S> subject;

        OperationObserver(final SerializedSubject<S, S> subject) {
            this.subject = subject;
        }

        @Override
        public void onNext(final PartialResponse response) {
            HillviewLogger.instance.info("OperationObserver onNext");
            S result = this.processResponse(response);
            this.subject.onNext(result);
        }

        @Override
        public void onError(final Throwable throwable) {
            HillviewLogger.instance.error("RemoteDataSet Observer received exception",
                    "{0}:{1}", RemoteDataSet.this.toString(),
                    Utilities.throwableToString(throwable));
            this.subject.onError(throwable);
        }

        @Override
        public void onCompleted() {
            HillviewLogger.instance.info("OperationObserver has completed");
            this.subject.onCompleted();
        }

        protected abstract S processResponse(final PartialResponse response);
    }

    /**
     * StreamObserver used by map() and zip() implementations above to point to instantiate
     * a new RemoteDataSet that points to a dataset on a remote server.
     */
    private class NewDataSetObserver<S> extends OperationObserver<PartialResult<IDataSet<S>>> {
        NewDataSetObserver(SerializedSubject<PartialResult<IDataSet<S>>, PartialResult<IDataSet<S>>> subject) {
            super(subject);
        }

        @Override
        @SuppressWarnings("unchecked")
        public PartialResult<IDataSet<S>> processResponse(final PartialResponse response) {
            final OperationResponse op = SerializationUtils.deserialize(response
                    .getSerializedOp().toByteArray());
            PartialResult<Integer> pr = Converters.checkNull((PartialResult<Integer>)op.result);
            final IDataSet<S> ids = (pr.deltaValue == null) ? null :
                    new RemoteDataSet<S>(RemoteDataSet.this.serverEndpoint, pr.deltaValue);
            HillviewLogger.instance.info("Receiving partial response: new dataset", "{0}", pr.deltaValue);
            return new PartialResult<IDataSet<S>>(pr.deltaDone, ids);
        }
    }

    /**
     * StreamObserver used by sketch() implementations above.
     */
    private class SketchObserver<S> extends OperationObserver<PartialResult<S>> {
        SketchObserver(final SerializedSubject<PartialResult<S>, PartialResult<S>> subject) {
            super(subject);
        }

        @Override
        @SuppressWarnings("unchecked")
        public PartialResult<S> processResponse(final PartialResponse response) {
            final OperationResponse op = SerializationUtils.deserialize(response
                    .getSerializedOp().toByteArray());
            assert op.result != null;
            HillviewLogger.instance.info("Receiving partial sketch result", "{0}", op.result);
            return (PartialResult<S>)op.result;
        }
    }

    /**
     * StreamObserver used by manage() implementations above.
     */
    private class ManageObserver extends
            OperationObserver<PartialResult<ControlMessage.StatusList>> {
        private final ControlMessage message;
        private final RemoteDataSet  dataSet;

        ManageObserver(SerializedSubject<PartialResult<ControlMessage.StatusList>,
                PartialResult<ControlMessage.StatusList>> subject,
                              ControlMessage message, RemoteDataSet dataSet) {
            super(subject);
            this.message = message;
            this.dataSet = dataSet;
        }

        @Override
        @SuppressWarnings("unchecked")
        public PartialResult<ControlMessage.StatusList> processResponse(
                final PartialResponse response) {
            final OperationResponse op = SerializationUtils.deserialize(response
                    .getSerializedOp().toByteArray());
            return (PartialResult<ControlMessage.StatusList>)Converters.checkNull(op.result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onCompleted() {
            ControlMessage.Status status = this.message.remoteAction(this.dataSet);
            if (status != null) {
                ControlMessage.StatusList list = new ControlMessage.StatusList(status);
                this.subject.onNext(new PartialResult<ControlMessage.StatusList>(0, list));
            }
            this.subject.onCompleted();
        }
    }
}
