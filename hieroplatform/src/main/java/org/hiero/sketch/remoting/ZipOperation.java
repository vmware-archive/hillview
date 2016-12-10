package org.hiero.sketch.remoting;

import akka.actor.ActorRef;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;

/**
 * Message type to initiate a zip command against two RemoteDataSets
 */
public class ZipOperation extends RemoteOperation implements Serializable {
    @NonNull
    public final ActorRef remoteActor;

    public ZipOperation(@NonNull final ActorRef remoteActor) {
        this.remoteActor = remoteActor;
    }
}