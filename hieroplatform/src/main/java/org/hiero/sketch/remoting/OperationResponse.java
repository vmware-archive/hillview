package org.hiero.sketch.remoting;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.UUID;

/**
 * Class used to wrap responses of map and sketch executions.
 * @param <T> Return type of the result
 */
public class OperationResponse<T> implements Serializable {
    public final T result;
    @Nonnull
    public final UUID id;
    @Nonnull
    public final ResponseType type;

    public OperationResponse(final T result, @Nonnull final UUID id,
                             @Nonnull final ResponseType type) {
        this.result = result;
        this.id = id;
        this.type = type;
    }

    public enum ResponseType {
        OnNext, OnCompletion, OnError, NewRemoteDataSet
    }
}
