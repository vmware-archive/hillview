package org.hiero.sketch.remoting;

import akka.actor.ActorRef;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Message type to initiate a zip command against two RemoteDataSets
 */
public class ZipOperation extends RemoteOperation implements Serializable {
    @Nonnull
    public final ActorRef remoteActor;

    public ZipOperation(@Nonnull final ActorRef remoteActor) {
        this.remoteActor = remoteActor;
    }
}