package org.hillview.remoting;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.PartialResult;
import org.hillview.pb.Ack;
import org.hillview.pb.Command;
import org.hillview.pb.HillviewServerGrpc;
import org.hillview.pb.PartialResponse;
import org.hillview.utils.Converters;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server that transfers map(), sketch(), zip() and unsubscribe() RPCs from a RemoteDataSet
 * object to locally managed IDataSet objects, and streams back results.
 */
public class HillviewServer extends HillviewServerGrpc.HillviewServerImplBase {
    public static final int DEFAULT_IDS_INDEX = 1;
    public static final int DEFAULT_PORT = 3569;
    private static final String LOCALHOST = "127.0.0.1";
    private static final int NUM_THREADS = 5;
    private static final Executor EXECUTOR = Executors.newFixedThreadPool(NUM_THREADS);
    private final Server server;
    private final AtomicInteger dsIndex = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, IDataSet> dataSets;
    private final ConcurrentHashMap<UUID, Subscription> operationToObservable
            = new ConcurrentHashMap<>();
    private final HostAndPort listenAddress;

    public HillviewServer(final HostAndPort listenAddress, final IDataSet dataSet) throws IOException {
        this.listenAddress = listenAddress;
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress(listenAddress.getHost(),
                                                                     listenAddress.getPort()))
                                        .executor(EXECUTOR)
                                        .addService(this)
                                        .build()
                                        .start();
        this.dataSets = new ConcurrentHashMap<>();
        this.dataSets.put(this.dsIndex.incrementAndGet(), dataSet);
    }

    private Subscriber<PartialResult<IDataSet>> createSubscriber(
            UUID id, final StreamObserver<PartialResponse> responseObserver) {
        return new Subscriber<PartialResult<IDataSet>>() {
            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
                HillviewServer.this.operationToObservable.remove(id);
            }

            @Override
            public void onError(final Throwable throwable) {
                throwable.printStackTrace();
                responseObserver.onError(throwable);
                HillviewServer.this.operationToObservable.remove(id);
            }

            @Override
            public void onNext(final PartialResult<IDataSet> pr) {
                Integer idsIndex = null;
                if (pr.deltaValue != null) {
                    idsIndex = HillviewServer.this.dsIndex.incrementAndGet();
                    HillviewServer.this.dataSets.put(idsIndex, Converters.checkNull(pr.deltaValue));
                }
                final OperationResponse<Integer> res = new OperationResponse<Integer>(idsIndex);
                final byte[] bytes = SerializationUtils.serialize(res);
                responseObserver.onNext(PartialResponse.newBuilder()
                                                       .setSerializedOp(ByteString.copyFrom(bytes)).build());
            }
        };
    }

    /**
     * Implementation of map() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void map(final Command command, final StreamObserver<PartialResponse> responseObserver) {
        System.out.println("Received command map");
        try {
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final MapOperation mapOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult<IDataSet>> observable =
                    this.dataSets.get(command.getIdsIndex())
                                 .map(mapOp.mapper);
            final Subscription sub = observable.subscribe(this.createSubscriber(mapOp.id, responseObserver));
            this.operationToObservable.put(mapOp.id, sub);
        } catch (final Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    /**
     * Implementation of flatMap() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(final Command command, final StreamObserver<PartialResponse>
            responseObserver) {
        System.out.println("Received command flatmap");
        try {
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final FlatMapOperation mapOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult<IDataSet>> observable =
                    this.dataSets.get(command.getIdsIndex())
                            .flatMap(mapOp.mapper);
            final Subscription sub = observable.subscribe(this.createSubscriber(mapOp.id, responseObserver));
            this.operationToObservable.put(mapOp.id, sub);
        } catch (final Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    /**
     * Implementation of sketch() service in hillview.proto.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void sketch(final Command command,
                       final StreamObserver<PartialResponse> responseObserver) {
        try {
            if (!this.checkValidIdsIndex(command.getIdsIndex(), responseObserver)) {
                return;
            }
            final byte[] bytes = command.getSerializedOp().toByteArray();
            final SketchOperation sketchOp = SerializationUtils.deserialize(bytes);
            final Observable<PartialResult> observable = this.dataSets.get(command.getIdsIndex())
                                                                      .sketch(sketchOp.sketch);
            final Subscription sub = observable.subscribe(new Subscriber<PartialResult>() {
                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                    HillviewServer.this.operationToObservable.remove(sketchOp.id);
                }

                @Override
                public void onError(final Throwable throwable) {
                    throwable.printStackTrace();
                    responseObserver.onError(throwable);
                    HillviewServer.this.operationToObservable.remove(sketchOp.id);
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
            this.operationToObservable.put(sketchOp.id, sub);
        } catch (final Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
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
            final IDataSet other = this.dataSets.get(zipOp.datasetIndex);
            final Observable<PartialResult<IDataSet>> observable =
                    this.dataSets.get(command.getIdsIndex())
                                 .zip(other);
            final Subscription sub = observable.subscribe(
                    this.createSubscriber(zipOp.id, responseObserver));
            this.operationToObservable.put(zipOp.id, sub);
        } catch (final Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
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
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    /**
     * shutdown RPC server
     */
    public void shutdown() {
        this.server.shutdown();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean checkValidIdsIndex(final int index,
                                       final StreamObserver<PartialResponse> observer) {
        if (!this.dataSets.containsKey(index)) {
            observer.onError(new RuntimeException("Table index does not exist: "
                    + index + " " + this.listenAddress));
            return false;
        }
        return true;
    }
}
