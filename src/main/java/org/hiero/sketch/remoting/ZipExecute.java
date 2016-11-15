package org.hiero.sketch.remoting;

import akka.actor.ActorRef;
import org.hiero.sketch.dataset.api.IDataSet;

import java.io.Serializable;

/**
 * Wraps the necessary references in order to execute a zip between
 * two IDataSets pointed to by two server-actors. It holds a reference
 * to the client-actor to which the results must be sent.
 * Triggered by a ZipOperation at a remote actor pointing a data-set.
 */
class ZipExecute implements Serializable{
    final ZipOperation zipOp;
    final IDataSet dataSet;
    final ActorRef clientActor;

    ZipExecute(final ZipOperation zipOperation,
               final IDataSet dataSet,
               final ActorRef sourceActor) {
        this.zipOp = zipOperation;
        this.dataSet = dataSet;
        this.clientActor = sourceActor;
    }
}
