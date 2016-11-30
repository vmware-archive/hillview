package org.hiero.sketch.remoting;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base type for remote operations with a unique ID
 */
public class RemoteOperation implements Serializable {
    @NonNull
    public final UUID id = UUID.randomUUID();
}