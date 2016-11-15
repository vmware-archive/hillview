package org.hiero.sketch.remoting;

import java.io.Serializable;
import java.util.UUID;

/**
 * Class used to wrap responses of map and sketch executions.
 * @param <T> Return type of the result
 */
public class OperationResponse<T> implements Serializable {
    public final T result;
    public final UUID id;
    public final Type type;

    public OperationResponse(final T result, final UUID id, final Type type) {
        this.result = result;
        this.id = id;
        this.type = type;
    }

    public enum Type {
        OnNext, OnCompletion, OnError, NewRemoteDataSet
    }
}
