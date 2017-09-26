/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.hillview.dataset.api.*;
import org.hillview.pb.Ack;
import org.hillview.pb.Command;
import org.hillview.pb.HillviewServerGrpc;
import org.hillview.pb.PartialResponse;
import org.hillview.remoting.*;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogging;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hillview.remoting.HillviewServer.DEFAULT_IDS_INDEX;

/**
 * An IDataSet that is a proxy for a DataSet on a remote machine. The remote IDataSet
 * is pointed to by (serverEndpoint, remoteHandle). Any RemoteDataSet instantiated
 * with a wrong value for either entry of the tuple will result in an exception.
 */
public class RemoteDataSet<T> extends BaseDataSet<T> {
    private final static int TIMEOUT = 60000 * 5;  // TODO: import via config file
    private final int remoteHandle;
    private final HostAndPort serverEndpoint;
    private final HillviewServerGrpc.HillviewServerStub stub;

    public RemoteDataSet(final HostAndPort serverEndpoint) {
        this(serverEndpoint, DEFAULT_IDS_INDEX);
    }

    public RemoteDataSet(final HostAndPort serverEndpoint, final int remoteHandle) {
        this.serverEndpoint = serverEndpoint;
        this.remoteHandle = remoteHandle;
        this.stub = HillviewServerGrpc.newStub(NettyChannelBuilder
                .forAddress(serverEndpoint.getHost(), serverEndpoint.getPort())
                .maxInboundMessageSize(HillviewServer.MAX_MESSAGE_SIZE)
                .usePlaintext(true)   // channel is unencrypted.
                .build());
    }

    /**
     * Map operations on a RemoteDataSet result in only one onNext
     * invocation that will return the final IDataSet.
     */
    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(final IMap<T, S> mapper) {
        final MapOperation<T, S> mapOp = new MapOperation<T, S>(mapper);
        final byte[] serializedOp = SerializationUtils.serialize(mapOp);
        final UUID operationId = UUID.randomUUID();
        final Command command = Command.newBuilder()
                                       .setIdsIndex(this.remoteHandle)
                                       .setSerializedOp(ByteString.copyFrom(serializedOp))
                                       .setHighId(operationId.getMostSignificantBits())
                                       .setLowId(operationId.getLeastSignificantBits())
                                       .build();
        final PublishSubject<PartialResult<IDataSet<S>>> subj = PublishSubject.create();
        final StreamObserver<PartialResponse> responseObserver = new NewDataSetObserver<S>(subj);
        return subj.doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                                                 .map(command, responseObserver))
                   .doOnUnsubscribe(() -> this.unsubscribe(operationId));
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> flatMap(IMap<T, List<S>> mapper) {
        final FlatMapOperation<T, S> mapOp = new FlatMapOperation<T, S>(mapper);
        final byte[] serializedOp = SerializationUtils.serialize(mapOp);
        final UUID operationId = UUID.randomUUID();
        final Command command = Command.newBuilder()
                .setIdsIndex(this.remoteHandle)
                .setSerializedOp(ByteString.copyFrom(serializedOp))
                .setHighId(operationId.getMostSignificantBits())
                .setLowId(operationId.getLeastSignificantBits())
                .build();
        final PublishSubject<PartialResult<IDataSet<S>>> subj = PublishSubject.create();
        final StreamObserver<PartialResponse> responseObserver = new NewDataSetObserver<S>(subj);
        return subj.doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                .flatMap(command, responseObserver))
                .doOnUnsubscribe(() -> this.unsubscribe(operationId));
    }

    /**
     * Sketch operation that streams partial results from the server to the caller.
     */
    @Override
    public <R> Observable<PartialResult<R>> sketch(final ISketch<T, R> sketch) {
        final SketchOperation<T, R> sketchOp = new SketchOperation<>(sketch);
        final byte[] serializedOp = SerializationUtils.serialize(sketchOp);
        final UUID operationId = UUID.randomUUID();
        final Command command = Command.newBuilder()
                                       .setIdsIndex(this.remoteHandle)
                                       .setSerializedOp(ByteString.copyFrom(serializedOp))
                                       .setHighId(operationId.getMostSignificantBits())
                                       .setLowId(operationId.getLeastSignificantBits())
                                       .build();
        final PublishSubject<PartialResult<R>> subj = PublishSubject.create();
        final StreamObserver<PartialResponse> responseObserver = new SketchObserver<>(subj);
        return subj.doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                                                 .sketch(command, responseObserver))
                   .doOnUnsubscribe(() -> this.unsubscribe(operationId));
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
        final byte[] serializedOp = SerializationUtils.serialize(zip);
        final UUID operationId = UUID.randomUUID();
        final Command command = Command.newBuilder()
                                         .setIdsIndex(this.remoteHandle)
                                         .setSerializedOp(ByteString.copyFrom(serializedOp))
                                         .setHighId(operationId.getMostSignificantBits())
                                         .setLowId(operationId.getLeastSignificantBits())
                                         .build();
        final PublishSubject<PartialResult<IDataSet<Pair<T, S>>>> subj = PublishSubject.create();
        final StreamObserver<PartialResponse> responseObserver =
                                                        new NewDataSetObserver<Pair<T, S>>(subj);
        return subj.doOnSubscribe(() -> this.stub.withDeadlineAfter(TIMEOUT, TimeUnit.MILLISECONDS)
                                                 .zip(command, responseObserver))
                   .doOnUnsubscribe(() -> this.unsubscribe(operationId));
    }

    /**
     * Unsubscribes an operation. This method is safe to invoke multiple times because the
     * logic on the remote end is idempotent.
     */
    private void unsubscribe(final UUID id) {
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
    private abstract static class OperationObserver<T> implements StreamObserver<PartialResponse> {
        final PublishSubject<T> subject;

        public OperationObserver(final PublishSubject<T> subject) {
            this.subject = subject;
        }

        @Override
        public void onNext(final PartialResponse response) {
            this.subject.onNext(processResponse(response));
        }

        @Override
        public void onError(final Throwable throwable) {
            HillviewLogging.logger.error("Caught exception", throwable);
            throwable.printStackTrace();
            this.subject.onError(throwable);
        }

        @Override
        public void onCompleted() {
            this.subject.onCompleted();
        }

        public abstract T processResponse(final PartialResponse response);
    }

    /**
     * StreamObserver used by map() and zip() implementations above to point to instantiate
     * a new RemoteDataSet that points to a dataset on a remote server.
     */
    private class NewDataSetObserver<S> extends OperationObserver<PartialResult<IDataSet<S>>> {
        public NewDataSetObserver(PublishSubject<PartialResult<IDataSet<S>>> subject) {
            super(subject);
        }

        @Override
        public PartialResult<IDataSet<S>> processResponse(final PartialResponse response) {
            final OperationResponse op = SerializationUtils.deserialize(response
                    .getSerializedOp().toByteArray());
            final IDataSet<S> ids = (op.result == null) ? null :
                    new RemoteDataSet<S>(RemoteDataSet.this.serverEndpoint, (int) op.result);
            return new PartialResult<IDataSet<S>>(ids);
        }
    }

    /**
     * StreamObserver used by test() implementation above.
     */
    private static class SketchObserver<S> extends OperationObserver<PartialResult<S>> {
        public SketchObserver(final PublishSubject<PartialResult<S>> subject) {
            super(subject);
        }

        @Override
        @SuppressWarnings("unchecked")
        public PartialResult<S> processResponse(final PartialResponse response) {
            final OperationResponse op = SerializationUtils.deserialize(response
                    .getSerializedOp().toByteArray());
            return (PartialResult<S>) Converters.checkNull(op.result);
        }
    }
}