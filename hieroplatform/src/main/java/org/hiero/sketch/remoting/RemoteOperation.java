package org.hiero.sketch.remoting;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.UUID;

/**
 * Base type for remote operations with a unique ID
 */
class RemoteOperation implements Serializable {

    public final UUID id = UUID.randomUUID();
}