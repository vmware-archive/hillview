package org.hiero.sketch.dataset.api;

/**
 * Used to report progress.
 */
public interface IProgressReporter {
    /**
     * Report the amount of work done (0 <= done <= 1)
     * @param done Work done.
     */
    void report(double done);
}
