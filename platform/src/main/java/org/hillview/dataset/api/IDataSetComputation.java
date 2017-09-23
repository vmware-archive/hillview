package org.hillview.dataset.api;

import java.io.Serializable;

/**
 * This is a common interface for all computations that execute on IDataSet objects.
 * It has no methods, but it inherits from Serializable.
 */
public interface IDataSetComputation extends Serializable {
    /**
     * We cannot override toString, so we implement a new method.
     */
    default String asString() { return this.getClass().getName(); }
}
