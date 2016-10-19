package org.hiero.sketch.dataset.api;

/**
 * Used to report the result of an asynchronous computation.
 * Also contains a progress reporter.
 * @param <T> Type of result reported.
 */
public interface IResultReporter<T> {
    /**
     * Report a partial result.
     * @param done How much of the result is computed (0 <= done <= 1).
     * @param data Current partial result.
     */
    void report(double done, T data);

    /**
     * Report a complete result.
     * @param data Final result.
     */
    void reportComplete(T data);

    /**
     * Report the fact that the computation has ended in an exception.
     * @param ex Exception that has terminated the execution.
     */
    void reportException(Exception ex);
}
