package org.hiero.sketch.dataset.api;

/**
 * Used to signal the desire to cancel an asynchronous computation.
 */
public interface CancellationToken {
    void cancel();
    boolean isCancelled();
}
