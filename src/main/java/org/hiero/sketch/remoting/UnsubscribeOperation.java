package org.hiero.sketch.remoting;

import java.io.Serializable;
import java.util.UUID;

/**
 * Unsubscribe to a remote Map/Sketch operation
 */
public class UnsubscribeOperation implements Serializable {
    public final UUID id;

    public UnsubscribeOperation(final UUID id) {
        this.id = id;
    }
}
