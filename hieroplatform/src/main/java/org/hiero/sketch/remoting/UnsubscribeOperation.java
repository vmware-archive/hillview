package org.hiero.sketch.remoting;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.UUID;

/**
 * Unsubscribe to a remote Map/Sketch operation
 */
class UnsubscribeOperation implements Serializable {
    @Nonnull
    public final UUID id;

    public UnsubscribeOperation(@Nonnull final UUID id) {
        this.id = id;
    }
}
