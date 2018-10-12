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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.stub.StreamObserver;
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
import org.hillview.utils.*;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server that transfers map(), sketch(), zip(), manage(), and unsubscribe() RPCs from a
 * RemoteDataSet object to locally managed IDataSet objects, and streams back results.
 * If memoization is enabled, it caches the results of (operation, dataset-index) types.
 */
public class HillviewServer extends HillviewServerGrpc.HillviewServerImplBase {
    public static final int DEFAULT_PORT = 3569;
    private static final int NUM_THREADS = 5;
    public static final int MAX_MESSAGE_SIZE = 5 * 20971520;
    // We run the executor service with high priority mainly so that
    // it can propagate unsubscriptions quickly.
    private final ExecutorService executorService =
            ExecutorUtils.newNamedThreadPool("server", NUM_THREADS, Thread.MAX_PRIORITY);
    private static final int EXPIRE_TIME_IN_HOURS = 2;
    private boolean MEMOIZE = true;

    // Using PollSelectorProvider() to avoid epoll CPU utilization problems.
    // See: https://github.com/netty/netty/issues/327
    // This code is not portable Java; the PollSelectorProvider seems
    // to be useful for microbenchmarks mostly.
    /*
    private final EventLoopGroup workerElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("worker"), new PollSelectorProvider());
    private final EventLoopGroup bossElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("boss"), new PollSelectorProvider());
    */
    private final EventLoopGroup workerElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("worker"));
    private final EventLoopGroup bossElg = new NioEventLoopGroup(1,
            ExecutorUtils.newFastLocalThreadFactory("boss"));
    private final Server server;
    private final AtomicInteger dsIndex = new AtomicInteger(1);

    /**
     * We reserve datasets with negative indexes.  These are never garbage-collected.
     */
    private final HashMap<Integer, IDataSet> initialDatasets;
    /**
     * Maps a dataset number to the actual dataset.  This is the only handle that
     * one can hold to an IDataSet on the server-side, so once an entry is removed
     * from the cache it can be GC-ed.  This is how memory is reclaimed.
     */
    private final Cache<Integer, IDataSet> dataSets;
    private final ConcurrentHashMap<UUID, Subscription> operationToObservable
            = new ConcurrentHashMap<UUID, Subscription>();
    /**
     * The timeline of a command can look like this:
     * --------#-------|------------#-----------|---------#----------
     *      unsub    received     unsub     completed   unsub
     * I.e., the unsubscribe request can be actually received before or
     * after the command.  We store information about unsubscribe requests
     * and commands in this cache for a while, to enable matching the
     * ones that show up out of order.
     *
     * Moreover, the unsub and receive messages can be processed on
     * separate threads, so we have to be careful with TOCTOU.
     */
    private final Cache<UUID, Boolean> toUnsubscribe;
    private final HostAndPort listenAddress;

    private final MemoizedResults memoizedCommands;

    public HillviewServer(final HostAndPort listenAddress, final IDataSet initialDataset) throws IOException {
        this.initialDatasets = new HashMap<Integer, IDataSet>();
        this.addInitialDataset(initialDataset);
        this.listenAddress = listenAddress;
        this.memoizedCommands = new MemoizedResults();
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress(listenAddress.getHost(),
                                                                     listenAddress.getPort()))
                                        .executor(executorService)
                                        .workerEventLoopGroup(workerElg)
                                        .bossEventLoopGroup(bossElg)
                                        .addService(this)
                                        .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                                        .build()
                                        .start();
        this.dataSets = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_TIME_IN_HOURS, TimeUnit.HOURS)
                .removalListener(
                        (RemovalListener<Integer, IDataSet>) removalNotification ->
                                HillviewLogger.instance.info("Removing reference to dataset", "{0}: {1}",
                                    removalNotification.getKey(), removalNotification.getValue().toString()))
                .build();
        this.toUnsubscribe = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_TIME_IN_HOURS, TimeUnit.HOURS)
                .build();
    }

    int addInitialDataset(final IDataSet initial) {
        int index = -this.initialDatasets.size() - 1;
        this.initialDatasets.put(index, initial);
        return index;
    }

    private UUID getId(Command command) {
        return new UUID(command.getHighId(), command.getLowId());
    }

    /**
     * Save the RxJava subscription for a command; allows it to be cancelled.
     * @param id              Command id.
     * @param subscription    RxJava subscription.
     * @param reason          Logging message.
     * @return                True if the operation is already cancelled.
     */
    synchronized private boolean saveSubscription(
            UUID id, Subscription subscription, String reason) {
        HillviewLogger.instance.info("Saving subscription", "{0}:{1}", reason, id);
        Boolean unsub = this.toUnsubscribe.getIfPresent(id);
        this.operationToObservable.put(id, subscription);
        if (unsub == null) {
            this.toUnsubscribe.put(id, false);
            return false;
        } else if (!unsub) {
            this.toUnsubscribe.invalidate(id);
        } else {
            this.toUnsubscribe.put(id, true);
        }
        return unsub;
    }

    @Nullable
    synchronized private Subscription removeSubscription(UUID id, String reason) {
        HillviewLogger.instance.info("Removing subscription", "{0}:{1}", reason, id);
        Boolean b = this.toUnsubscribe.getIfPresent(id);
        if (b != null)
            this.toUnsubscribe.invalidate(id);
        else
            this.toUnsubscribe.put(id, true);
        return this.operationToObservable.remove(id);
    }

    synchronized private int save(IDataSet dataSet) {
        int index = this.dsIndex.getAndIncrement();
        if (index < 0)
            index = 0;
        HillviewLogger.instance.info("Inserting dataset", "{0}", index);
        if (this.dataSets.getIfPresent(index) != null)
            // This means we have created more than 2B datasets which haven't expired yet!
            throw new RuntimeException("Dataset index overflow: " + index);
        this.dataSets.put(index, dataSet);
        return index;
    }

    /**
     * Retrieve the dataset with the specified index.
     * @param index     Dataset index.
     * @param observer  Observer that is notified if the dataset is not available.
     * @return          The dataset, or null if there is no such dataset.
     */
    @Nullable
    synchronized private IDataSet getIfValid(final int index,
                                             final StreamObserver<PartialResponse> observer) {
        if (index < 0)
            return this.initialDatasets.get(index);
        IDataSet ds = this.dataSets.getIfPresent(index);
        if (ds == null)
            observer.onError(asStatusRuntimeException(
                    new DatasetMissing(index, this.listenAddress)));
        return ds;
    }

    /**
     * Delete all stored datasets (except the initial ones).
     * @return The number of deleted datasets.
     */
    synchronized public int deleteAllDatasets() {
        long removed = this.dataSets.size();
        this.dataSets.invalidateAll();
        this.memoizedCommands.clear();
        return (int)removed;
    }

    public void purgeMemoized() {
        this.memoizedCommands.clear();
    }

    /**
     * Change memoization policy.
     */
    public void setMemoization(boolean to) {
        this.MEMOIZE = to;
    }

    /**
     * Subscriber that handles map, flatMap and zip.
     */
    private Subscriber<PartialResult<IDataSet>> createSubscriber(
            final Command command, final UUID id, final String operation,
            final StreamObserver<PartialResponse> responseObserver) {
        return new Subscriber<PartialResult<IDataSet>>() {
            @Nullable private PartialResponse memoizedResult = null;
            @Nullable private Integer memoizedDatasetIndex = null;
            private CompletableFuture queue = CompletableFuture.completedFuture(null);

            @Override
            public void onCompleted() {
                queue = queue.thenRunAsync(() -> {
                    if (MEMOIZE && this.memoizedResult != null) {
                        HillviewServer.this.memoizedCommands.insert(
                                command, this.memoizedResult,
                                Converters.checkNull(this.memoizedDatasetIndex));
                    }
                    responseObserver.onCompleted();
                    HillviewServer.this.removeSubscription(id, operation + " completed");
                }, executorService);
            }

            @Override
            public void onError(final Throwable e) {
                queue = queue.thenRunAsync(() -> {
                    HillviewLogger.instance.error("Error when creating subscriber", e);
                    e.printStackTrace();
                    responseObserver.onError(asStatusRuntimeException(e));
                    HillviewServer.this.removeSubscription(id, operation + " on error");
                }, executorService);
            }

            @Override
            public void onNext(final PartialResult<IDataSet> pr) {
                queue = queue.thenRunAsync(() -> {
                    Integer idsIndex = null;
                    if (pr.deltaValue != null) {
                        idsIndex = HillviewServer.this.save(pr.deltaValue);
                    }
                    final OperationResponse<PartialResult<Integer>> res = new
                            OperationResponse<PartialResult<Integer>>(new
                            PartialResult<Integer>(pr.deltaDone, idsIndex));
                    final byte[] bytes = SerializationUtils.serialize(res);
                    final PartialResponse result = PartialResponse.newBuilder()
                            .setSerializedOp(ByteString.copyFrom(bytes)).build();
                    if (MEMOIZE) {
                        this.memoizedResult = result;
                        this.memoizedDatasetIndex = idsIndex;
                    }
                    responseObserver.onNext(result);
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
            final UUID commandId = this.getId(command);
            final IDataSet dataset = this.getIfValid(command.getIdsIndex(), responseObserver);
            if (dataset == null)
                return;
            final byte[] bytes = command.getSerializedOp().toByteArray();
            if (this.respondIfReplyIsMemoized(command, responseObserver, true)) {
                HillviewLogger.instance.info(
                        "Found memoized map", "on IDataSet#{0}", command.getIdsIndex());
                return;
            }

            final MapOperation mapOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult<IDataSet>> observable = dataset.map(mapOp.mapper);
            Subscriber subscriber = this.createSubscriber(
                    command, commandId, "map", responseObserver);
            final Subscription sub = observable
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                    .subscribe(subscriber);
            boolean unsub = this.saveSubscription(commandId, sub, "map");
            if (unsub)
                sub.unsubscribe();
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
    public void flatMap(
            final Command command, final StreamObserver<PartialResponse> responseObserver) {
        try {
            final UUID commandId = this.getId(command);
            final IDataSet dataset = this.getIfValid(command.getIdsIndex(), responseObserver);
            if (dataset == null)
                return;
            final byte[] bytes = command.getSerializedOp().toByteArray();

            if (this.respondIfReplyIsMemoized(command, responseObserver, true)) {
                HillviewLogger.instance.info(
                        "Found memoized flatMap", "on IDataSet#{0}", command.getIdsIndex());
                return;
            }
            final FlatMapOperation mapOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult<IDataSet>> observable = dataset.flatMap(mapOp.mapper);
            Subscriber subscriber = this.createSubscriber(
                    command, commandId, "flatMap", responseObserver);
            final Subscription sub = observable
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                    .subscribe(subscriber);
            boolean unsub = this.saveSubscription(commandId, sub, "flatMap");
            if (unsub)
                sub.unsubscribe();
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
            final UUID commandId = this.getId(command);
            boolean memoize = MEMOIZE;  // The value may change while we execute
            final IDataSet dataset = this.getIfValid(command.getIdsIndex(), responseObserver);
            if (dataset == null)
                return;
            if (this.respondIfReplyIsMemoized(command, responseObserver, false)) {
                HillviewLogger.instance.info(
                        "Found memoized sketch", "on IDataSet#{0}", command.getIdsIndex());
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final SketchOperation sketchOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult> observable = dataset.sketch(sketchOp.sketch);
            Subscriber subscriber = new Subscriber<PartialResult>() {
                @Nullable private Object sketchResultAccumulator =
                        memoize ? sketchOp.sketch.getZero(): null;
                private CompletableFuture queue = CompletableFuture.completedFuture(null);

                @Override
                public void onCompleted() {
                    queue = queue.thenRunAsync(() -> {
                        responseObserver.onCompleted();
                        HillviewServer.this.removeSubscription(commandId, "sketch completed");

                        if (memoize && this.sketchResultAccumulator != null) {
                            final OperationResponse<PartialResult> res =
                                    new OperationResponse<PartialResult>(
                                            new PartialResult(1.0, this.sketchResultAccumulator));
                            final byte[] bytes = SerializationUtils.serialize(res);
                            final PartialResponse memoizedResult = PartialResponse.newBuilder()
                                    .setSerializedOp(ByteString.copyFrom(bytes))
                                    .build();
                            HillviewServer.this.memoizedCommands.insert(command, memoizedResult, 0);
                        }
                    }, executorService);
                }

                @Override
                public void onError(final Throwable e) {
                    queue = queue.thenRunAsync(() -> {
                        HillviewLogger.instance.error("Exception in sketch", e);
                        e.printStackTrace();
                        responseObserver.onError(asStatusRuntimeException(e));
                        HillviewServer.this.removeSubscription(commandId, "sketch onError");
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
            };
            final Subscription sub = observable
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                    .subscribe(subscriber);
            boolean unsub = this.saveSubscription(commandId, sub, "sketch");
            if (unsub)
                sub.unsubscribe();
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
            final UUID commandId = this.getId(command);
            // TODO: handle errors in a better way in manage commands
            final IDataSet dataset = this.getIfValid(command.getIdsIndex(), responseObserver);
            if (dataset == null)
                return;
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final ManageOperation manage = SerializationUtils.deserialize(bytes);
            Observable<PartialResult<ControlMessage.StatusList>> observable = dataset.manage(manage
                    .message);
            final Callable<ControlMessage.StatusList> callable = () -> {
                HillviewLogger.instance.info("Starting manage", "{0}", manage.message.toString());
                ControlMessage.Status status;
                try {
                    status = manage.message.remoteServerAction(this);
                } catch (final Throwable t) {
                    status = new ControlMessage.Status("Exception", t);
                }
                ControlMessage.StatusList result = new ControlMessage.StatusList(status);
                HillviewLogger.instance.info("Completed manage", "{0}", manage.message.toString());
                return result;
            };
            Observable<JsonList<ControlMessage.Status>> executed = Observable.fromCallable(callable);
            observable = observable.mergeWith(executed.map(l -> new PartialResult(0, l)));

            Subscriber subscriber = new Subscriber<PartialResult<ControlMessage.StatusList>>() {
                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                    HillviewServer.this.removeSubscription(commandId, "manage completed");
                }

                @Override
                public void onError(final Throwable e) {
                    HillviewLogger.instance.error("Exception in manage operation", e);
                    e.printStackTrace();
                    responseObserver.onError(e);
                    HillviewServer.this.removeSubscription(commandId, "manage onError");
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
            };
            // Results of management commands are never memoized.
            final Subscription sub = observable
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                    .subscribe(subscriber);
            boolean unsub = this.saveSubscription(commandId, sub, "manage");
            if (unsub)
                sub.unsubscribe();
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
            final UUID commandId = this.getId(command);
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final ZipOperation zipOp = SerializationUtils.deserialize(bytes);
            final IDataSet left = this.getIfValid(command.getIdsIndex(), responseObserver);
            if (left == null)
                return;
            final IDataSet right = this.getIfValid(zipOp.datasetIndex, responseObserver);
            if (right == null)
                return;
            if (this.respondIfReplyIsMemoized(command, responseObserver, true)) {
                HillviewLogger.instance.info(
                        "Found memoized zip", "on IDataSet#{0}",
                        command.getIdsIndex());
                return;
            }

            final Observable<PartialResult<IDataSet>> observable = left.zip(right);
            Subscriber subscriber = this.createSubscriber(
                    command, commandId, "zip", responseObserver);
            final Subscription sub = observable
                    .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                    .subscribe(subscriber);
            boolean unsub = this.saveSubscription(commandId, sub, "zip");
            if (unsub)
                sub.unsubscribe();
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
            HillviewLogger.instance.info("Unsubscribing", "{0}", unsubscribeOp.id);
            @Nullable
            final Subscription subscription = this.removeSubscription(unsubscribeOp.id,
                    "unsubscribe request");
            if (subscription != null) {
                subscription.unsubscribe();
            } else {
                HillviewLogger.instance.warn("Could not find subscription", "{0}", unsubscribeOp.id);
                this.toUnsubscribe.put(unsubscribeOp.id, true);
            }
        } catch (final Exception e) {
            HillviewLogger.instance.error("Exception in unsubscribe", e);
            responseObserver.onError(asStatusRuntimeException(e));
        }
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

    /**
     * Respond with a memoized result if it is available.  Otherwise do nothing.
     * @param command   Command to execute.
     * @param responseObserver Observer that expects the result of the command.
     * @param checkResult      Only used if the result is actually a dataset id;
     *                         if the dataset with this id does not exist, then
     *                         it is removed from the memoization cache.  It means
     *                         that the dataset has expired.
     */
    private boolean respondIfReplyIsMemoized(final Command command,
                                             StreamObserver<PartialResponse> responseObserver,
                                             boolean checkResult) {
        if (!MEMOIZE)
            return false;
        MemoizedResults.ResponseAndId memoized = this.memoizedCommands.get(command);
        if (memoized == null)
            return false;
        if (checkResult) {
            int index = memoized.localDatasetIndex;
            assert index != 0;
            IDataSet ds = this.dataSets.getIfPresent(index);
            if (ds == null) {
                // This dataset no longer exists; remove it from
                // the memoization cache as well.
                this.memoizedCommands.remove(command, memoized);
                return false;
            }
        }
        responseObserver.onNext(memoized.response);
        responseObserver.onCompleted();
        return true;
    }

    /**
     * Helper method to propagate exceptions via gRPC
     */
    private StatusRuntimeException asStatusRuntimeException(final Throwable e) {
        final String stackTrace = ExceptionUtils.getStackTrace(e);
        return Status.INTERNAL.withDescription(stackTrace).asRuntimeException();
    }
}
