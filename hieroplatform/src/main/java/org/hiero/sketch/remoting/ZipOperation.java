package org.hiero.sketch.remoting;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Message type to initiate a zip command against two RemoteDataSets
 */
public class ZipOperation extends RemoteOperation implements Serializable {
    public final ActorRef remoteActor;

    public ZipOperation(final ActorRef remoteActor) {
        this.remoteActor = remoteActor;
    }
}