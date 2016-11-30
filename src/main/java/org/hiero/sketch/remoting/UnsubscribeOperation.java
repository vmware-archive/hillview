package org.hiero.sketch.remoting;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.UUID;

/**
 * Unsubscribe to a remote Map/Sketch operation
 */
public class UnsubscribeOperation implements Serializable {
    @NonNull
    public final UUID id;

    public UnsubscribeOperation(@NonNull final UUID id) {
        this.id = id;
    }
}
