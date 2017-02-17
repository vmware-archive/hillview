package org.hiero.sketch.dataset.api;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

/**
 * A monoid structure.
 * @param <R> Type of data representing an element of the monoid.
 */
public interface IMonoid<R> extends Serializable {
    @Nullable
    R zero();
    @Nullable R add(@Nullable R left, @Nullable R right);

    @Nullable default R reduce(List<R> data) {
        // This implementation avoids allocating a zero
        // if the list is non-empty.
        if (data.isEmpty())
            return this.zero();

        R result = data.get(0);
        // add the rest of the elements
        for (int i = 1; i < data.size(); i++)
            result = this.add(result, data.get(i));
        return result;
    }
}
