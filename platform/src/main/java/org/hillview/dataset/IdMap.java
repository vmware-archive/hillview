package org.hillview.dataset;

import org.hillview.dataset.api.IMap;

import javax.annotation.Nullable;

/**
 * Identity map.
 * @param <T>  Type of data.
 */
public class IdMap<T> implements IMap<T, T> {
    @Nullable
    @Override
    public T apply(@Nullable T data) {
        return data;
    }
}
