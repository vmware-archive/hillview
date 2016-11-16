package org.hiero.sketch.remoting;

import org.hiero.sketch.dataset.api.IMap;

import java.io.Serializable;

/**
 * Wrap an IMap object to be sent to a remote node
 * @param <T> Input type of the map function
 * @param <S> Output type of the map function
 */
public class MapOperation<T, S> extends RemoteOperation implements Serializable {
    public final IMap<T, S> mapper;

    public MapOperation(final IMap<T, S> mapper) {
        this.mapper = mapper;
    }
}
