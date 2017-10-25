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

package org.hillview.dataset.remoting;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hillview.dataset.api.DatasetMissing;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.PartialResult;
import org.hillview.dataset.api.ControlMessage;
import org.hillview.pb.Ack;
import org.hillview.pb.Command;
import org.hillview.pb.HillviewServerGrpc;
import org.hillview.pb.PartialResponse;
import org.hillview.utils.ExecutorUtils;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.JsonList;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import sun.nio.ch.PollSelectorProvider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server that transfers map(), sketch(), zip() and unsubscribe() RPCs from a RemoteDataSet
 * object to locally managed IDataSet objects, and streams back results.
 *
 * If memoization is enabled, it caches the results of (operation, dataset-index) types.
 */
public class HillviewServer extends HillviewServerGrpc.HillviewServerImplBase {
    /**
     * Index of remote initial dataset, containing just the Empty object.
     */
    public static final int ROOT_DATASET_INDEX = 0;
    public static final int DEFAULT_PORT = 3569;
    private static final String LOCALHOST = "127.0.0.1";
    private static final int NUM_THREADS = 5;
    public static final int MAX_MESSAGE_SIZE = 20971520;
    private final ExecutorService executorService = ExecutorUtils.newNamedThreadPool("server", NUM_THREADS);

    // Using PollSelectorProvider() to avoid Epoll CPU utilization problems.
    // See: https://github.com/netty/netty/issues/327
    private final EventLoopGroup workerElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("worker"), new PollSelectorProvider());
    private final EventLoopGroup bossElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("boss"), new PollSelectorProvider());
    private final Server server;
    private final AtomicInteger dsIndex = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, IDataSet> dataSets;
    private final ConcurrentHashMap<UUID, Subscription> operationToObservable
            = new ConcurrentHashMap<UUID, Subscription>();
    private final HostAndPort listenAddress;
    private final ConcurrentHashMap<ByteString, Map<Integer, PartialResponse>> memoizedCommands
            = new ConcurrentHashMap<ByteString, Map<Integer, PartialResponse>>();
    @SuppressWarnings("CanBeFinal")
    private boolean MEMOIZE = true;

    public HillviewServer(final HostAndPort listenAddress, final IDataSet dataSet) throws IOException {
        this.listenAddress = listenAddress;
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress(listenAddress.getHost(),
                                                                     listenAddress.getPort()))
                                        .executor(executorService)
                                        .workerEventLoopGroup(workerElg)
                                        .bossEventLoopGroup(bossElg)
                                        .addService(this)
                                        .maxMessageSize(MAX_MESSAGE_SIZE)
                                        .build()
                                        .start();
        this.dataSets = new ConcurrentHashMap<Integer, IDataSet>();
        this.put(this.dsIndex.getAndIncrement(), dataSet);
    }

    synchronized private IDataSet get(int index) {
        return this.dataSets.get(index);
    }

    synchronized private void put(int index, IDataSet dataSet) {
        HillviewLogger.instance.info("Inserting dataset", "{0}", index);
        this.dataSets.put(index, dataSet);
    }

    synchronized private void remove(int index) {
        HillviewLogger.instance.info("Removing dataset", "{0}", index);
        this.dataSets.remove(index);
    }

    /**
     * Change memoization.
     * @return Current state of memoization.
     */
    public boolean toggleMemoization() {
        this.MEMOIZE = !this.MEMOIZE;
        return this.MEMOIZE;
    }

    private Subscriber<PartialResult<IDataSet>> createSubscriber(final Command command,
            final UUID id, final StreamObserver<PartialResponse> responseObserver) {
        return new Subscriber<PartialResult<IDataSet>>() {
            @Nullable private PartialResponse memoizedResult = null;
            private CompletableFuture queue = CompletableFuture.completedFuture(null);

            @Override
            public void onCompleted() {
                queue = queue.thenRunAsync(() -> {
                    responseObserver.onCompleted();
                    HillviewServer.this.operationToObservable.remove(id);
                    if (MEMOIZE && this.memoizedResult != null) {
                        HillviewServer.this.memoizedCommands.computeIfAbsent(command.getSerializedOp(),
                                (k) -> new ConcurrentHashMap<>())
                                .put(command.getIdsIndex(), this.memoizedResult);
                    }
                }, executorService);
            }

            @Override
            public void onError(final Throwable e) {
                queue = queue.thenRunAsync(() -> {
                    HillviewLogger.instance.error("Error when creating subscriber", e);
                    e.printStackTrace();
                    responseObserver.onError(asStatusRuntimeException(e));
                    HillviewServer.this.operationToObservable.remove(id);
                }, executorService);
            }

            @Override
            public void onNext(final PartialResult<IDataSet> pr) {
                queue = queue.thenRunAsync(() -> {
                    Integer idsIndex = null;
                    if (pr.deltaValue != null) {
                        idsIndex = HillviewServer.this.dsIndex.getAndIncrement();
                        HillviewServer.this.put(idsIndex, pr.deltaValue);
                    }
                    final OperationResponse<Integer> res = new OperationResponse<Integer>(idsIndex);
                    final byte[] bytes = SerializationUtils.serialize(res);
                    final PartialResponse result = PartialResponse.newBuilder()
                            .setSerializedOp(ByteString.copyFrom(bytes)).build();
                    responseObserver.onNext(result);
                    if (MEMOIZE) {
                        this.memoizedResult = result;
                    }
                }, executorService);
            }
        };
    }

    /**
     * Implementation of map() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void map(final Command command, final StreamObserver<PartialResponse> responseObserver) {
        try {
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            if (this.respondIfReplyIsMemoized(command, responseObserver)) {
                HillviewLogger.instance.info(
                        "Found memoized map", "IDataSet#{0}", command.getIdsIndex());
                return;
            }

            final MapOperation mapOp = SerializationUtils.deserialize(bytes);
            final UUID commandId = new UUID(command.getHighId(), command.getLowId());
            final Observable<PartialResult<IDataSet>> observable =
                    this.get(command.getIdsIndex()).map(mapOp.mapper);
            final Subscription sub = observable.subscribe(
                    this.createSubscriber(command, commandId, responseObserver));
            this.operationToObservable.put(commandId, sub);
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in map", e);
            e.printStackTrace();
            responseObserver.onError(asStatusRuntimeException(e));
        }
    }

    /**
     * Implementation of flatMap() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(final Command command, final StreamObserver<PartialResponse>
            responseObserver) {
        try {
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();

            if (this.respondIfReplyIsMemoized(command, responseObserver)) {
                HillviewLogger.instance.info(
                        "Found memoized flatMap", "IDataSet#{0}", command.getIdsIndex());
                return;
            }
            final FlatMapOperation mapOp = SerializationUtils.deserialize(bytes);
            final UUID commandId = new UUID(command.getHighId(), command.getLowId());
            final Observable<PartialResult<IDataSet>> observable =
                    this.get(command.getIdsIndex())
                            .flatMap(mapOp.mapper);
            final Subscription sub = observable.subscribe(
                    this.createSubscriber(command, commandId, responseObserver));
            this.operationToObservable.put(commandId, sub);
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in flatMap", e);
            e.printStackTrace();
            responseObserver.onError(asStatusRuntimeException(e));
        }
    }

    /**
     * Implementation of sketch() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void sketch(final Command command, final StreamObserver<PartialResponse> responseObserver) {
        try {
            boolean memoize = MEMOIZE;  // The value may change while we execute
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            if (this.respondIfReplyIsMemoized(command, responseObserver)) {
                HillviewLogger.instance.info(
                        "Found memoized sketch", "IDataSet#{0}", command.getIdsIndex());
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final SketchOperation sketchOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult> observable = this.get(command.getIdsIndex())
                                                                      .sketch(sketchOp.sketch);
            final UUID commandId = new UUID(command.getHighId(), command.getLowId());
            final Subscription sub = observable.subscribe(new Subscriber<PartialResult>() {
                @Nullable private Object sketchResultAccumulator = memoize ? sketchOp.sketch.getZero(): null;
                private CompletableFuture queue = CompletableFuture.completedFuture(null);

                @Override
                public void onCompleted() {
                    queue = queue.thenRunAsync(() -> {
                        responseObserver.onCompleted();
                        HillviewServer.this.operationToObservable.remove(commandId);

                        if (memoize && this.sketchResultAccumulator != null) {
                            final OperationResponse<PartialResult> res =
                                    new OperationResponse<PartialResult>(new PartialResult(1.0, this.sketchResultAccumulator));
                            final byte[] bytes = SerializationUtils.serialize(res);
                            final PartialResponse memoizedResult = PartialResponse.newBuilder()
                                    .setSerializedOp(ByteString.copyFrom(bytes))
                                    .build();
                            HillviewServer.this.memoizedCommands.computeIfAbsent(command.getSerializedOp(),
                                    (k) -> new ConcurrentHashMap<Integer, PartialResponse>())
                                    .put(command.getIdsIndex(), memoizedResult);
                        }
                    }, executorService);
                }

                @Override
                public void onError(final Throwable e) {
                    queue = queue.thenRunAsync(() -> {
                        HillviewLogger.instance.error("Exception in sketch", e);
                        e.printStackTrace();
                        responseObserver.onError(asStatusRuntimeException(e));
                        HillviewServer.this.operationToObservable.remove(commandId);
                    }, executorService);
                }

                @Override
                public void onNext(final PartialResult pr) {
                    queue = queue.thenRunAsync(() -> {
                        if (memoize && this.sketchResultAccumulator != null)
                            this.sketchResultAccumulator = sketchOp.sketch.add(this
                                    .sketchResultAccumulator, pr.deltaValue);
                        final OperationResponse<PartialResult> res =
                                new OperationResponse<PartialResult>(pr);
                        final byte[] bytes = SerializationUtils.serialize(res);
                        responseObserver.onNext(PartialResponse.newBuilder()
                                .setSerializedOp(ByteString.copyFrom(bytes))
                                .build());
                    }, executorService);
                }
            });
            this.operationToObservable.put(commandId, sub);
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in sketch", e);
            e.printStackTrace();
            responseObserver.onError(asStatusRuntimeException(e));
        }
    }

    /**
     * Implementation of manage() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void manage(Command command, StreamObserver<PartialResponse> responseObserver) {
        try {
            // TODO: handle errors in a better way in manage commands
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final ManageOperation manage = SerializationUtils.deserialize(bytes);
            final UUID commandId = new UUID(command.getHighId(), command.getLowId());
            Observable<PartialResult<JsonList<ControlMessage.Status>>> observable =
                    this.get(command.getIdsIndex()).manage(manage.message);
            final Callable<JsonList<ControlMessage.Status>> callable = () -> {
                HillviewLogger.instance.info("Starting manage", "{0}", manage.message.toString());
                ControlMessage.Status status;
                try {
                    status = manage.message.remoteServerAction(this);
                } catch (final Throwable t) {
                    status = new ControlMessage.Status("Exception", t);
                }
                JsonList<ControlMessage.Status> result = new JsonList<ControlMessage.Status>();
                if (status != null)
                    result.add(status);
                HillviewLogger.instance.info("Completed manage", "{0}", manage.message.toString());
                return result;
            };
            Observable<JsonList<ControlMessage.Status>> executed = Observable.fromCallable(callable);
            observable = observable.mergeWith(executed.map(l -> new PartialResult(0, l)));

            final Subscription sub = observable.subscribe(
                    new Subscriber<PartialResult<JsonList<ControlMessage.Status>>>() {
                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                            HillviewServer.this.operationToObservable.remove(commandId);
                        }

                        @Override
                        public void onError(final Throwable e) {
                            HillviewLogger.instance.error("Exception in manage operation", e);
                            e.printStackTrace();
                            responseObserver.onError(e);
                            HillviewServer.this.operationToObservable.remove(commandId);
                        }

                        @Override
                        public void onNext(final PartialResult pr) {
                            final OperationResponse<PartialResult> res =
                                    new OperationResponse<PartialResult>(pr);
                            final byte[] bytes = SerializationUtils.serialize(res);
                            responseObserver.onNext(PartialResponse.newBuilder()
                                    .setSerializedOp(ByteString.copyFrom(bytes))
                                    .build());
                        }
            });
            this.operationToObservable.put(commandId, sub);
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in manage", e);
            e.printStackTrace();
        }
    }

    /**
     * Implementation of zip() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void zip(final Command command, final StreamObserver<PartialResponse> responseObserver) {
        try {
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final ZipOperation zipOp = SerializationUtils.deserialize(bytes);
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)
                    || !this.checkValidIdsIndex(zipOp.datasetIndex, responseObserver)) {
                return;
            }
            if (this.respondIfReplyIsMemoized(command, responseObserver)) {
                HillviewLogger.instance.info(
                        "Found memoized zip", "IDataSet#{0}",
                        command.getIdsIndex());
                return;
            }

            final IDataSet other = this.get(zipOp.datasetIndex);
            final Observable<PartialResult<IDataSet>> observable =
                    this.get(command.getIdsIndex()).zip(other);
            final UUID commandId = new UUID(command.getHighId(), command.getLowId());
            final Subscription sub = observable.subscribe(
                    this.createSubscriber(command, commandId, responseObserver));
            this.operationToObservable.put(commandId, sub);
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in zip", e);
            e.printStackTrace();
            responseObserver.onError(asStatusRuntimeException(e));
        }
    }

    /**
     * Implementation of unsubscribe() service in hillview.proto.
     */
    @Override
    public void unsubscribe(final Command command, final StreamObserver<Ack> responseObserver) {
        try {
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final UnsubscribeOperation unsubscribeOp = SerializationUtils.deserialize(bytes);
            final Subscription subscription = this.operationToObservable.remove(unsubscribeOp.id);
            if (subscription != null) {
                subscription.unsubscribe();
            }
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in unsubscribe", e);
            responseObserver.onError(asStatusRuntimeException(e));
        }
    }

    /**
     * Purges all memoized results
     */
    public void purgeCache() {
        this.memoizedCommands.clear();
    }

    /**
     * Delete all stored datasets (except the one with number 0).
     * @return The number of deleted datasets.
     */
    public int deleteAllDatasets() {
        // TODO: this will eventually be replaced with a smarter GC policy.
        int removed = 0;
        List<Integer> list = new ArrayList<Integer>(this.dataSets.keySet());
        for (int i : list) {
            if (i == ROOT_DATASET_INDEX)
                continue;
            this.remove(i);
            removed++;
        }
        if (this.dataSets.size() == 0)
            throw new RuntimeException("Cannot find initial dataset");

        // This is necessary because otherwise we will have stale
        // dataset ids in the cache, which will get returned incorrectly.
        // Whatever we do for GC will have to keep a consistent map
        // so that no stale dataset ids are in the memoization cache
        // if they are not in the list of datasets.
        this.purgeCache();

        return removed;
    }

    /**
     * shutdown RPC server
     */
    public void shutdown() {
        this.server.shutdown();
        this.workerElg.shutdownGracefully();
        this.bossElg.shutdownGracefully();
        this.executorService.shutdownNow();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean checkValidIdsIndex(final int index,
                                       final StreamObserver<PartialResponse> observer) {
        if (!this.dataSets.containsKey(index)) {
            observer.onError(asStatusRuntimeException(
                    new DatasetMissing(index, this.listenAddress)));
            return false;
        }
        return true;
    }

    /**
     * Respond with a memoized result if it is available.
     */
    private boolean respondIfReplyIsMemoized(final Command command,
                                             final StreamObserver<PartialResponse> responseObserver) {
        if (MEMOIZE && this.memoizedCommands.containsKey(command.getSerializedOp())
             && this.memoizedCommands.get(command.getSerializedOp()).containsKey(command.getIdsIndex())) {
            responseObserver.onNext(this.memoizedCommands.get(command.getSerializedOp()).get(command.getIdsIndex()));
            responseObserver.onCompleted();
            return true;
        }
        return false;
    }

    /**
     * Helper method to propagate exceptions via gRPC
     */
    private StatusRuntimeException asStatusRuntimeException(final Throwable e) {
        final String stackTrace = ExceptionUtils.getStackTrace(e);
        return Status.INTERNAL.withDescription(stackTrace).asRuntimeException();
    }
}
