package org.hiero.sketch.remoting;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.UUID;

/**
 * Class used to wrap responses of map and sketch executions.
 * @param <T> Return type of the result
 */
public class OperationResponse<T> implements Serializable {
    public final T result;
    @NonNull
    public final UUID id;
    @NonNull
    public final ResponseType type;

    public OperationResponse(final T result, @NonNull final UUID id,
                             @NonNull final ResponseType type) {
        this.result = result;
        this.id = id;
        this.type = type;
    }

    public enum ResponseType {
        OnNext, OnCompletion, OnError, NewRemoteDataSet
    }
}
