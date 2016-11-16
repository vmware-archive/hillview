package org.hiero.sketch.remoting;

import org.hiero.sketch.dataset.api.ISketch;

import java.io.Serializable;

/**
 * Wrap an ISketch object to be sent to a remote node
 * @param <T> Input type of the sketch function
 * @param <R> Output type of the sketch function
 */
public class SketchOperation<T, R> extends RemoteOperation implements Serializable {
    public final ISketch<T, R> sketch;

    public SketchOperation(final ISketch<T, R> sketch) {
        this.sketch = sketch;
    }
}
