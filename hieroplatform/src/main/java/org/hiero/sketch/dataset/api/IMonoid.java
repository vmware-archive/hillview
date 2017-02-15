package org.hiero.sketch.dataset.api;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

/**
 * A monoid structure.
 * @param <R> Type of data representing an element of the monoid.
 */
public interface IMonoid<R> extends Serializable {
    R zero();
    R add(R left, R right);

    default R reduce(List<R> data) {
        // This implementation avoids allocating a zero
        // if the list is non-empty.
        if (data.isEmpty())
            return this.zero();

        R result = data.get(0);
        for (int i=0; i < data.size(); i++)
            result = this.add(result, data.get(i));
        return result;
    }
}
