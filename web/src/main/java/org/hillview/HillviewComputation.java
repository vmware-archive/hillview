/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.RpcTargetAction;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents information that has led to the creation of an RpcTarget object.
 * The information stores the inputs and the request that cause the creation of the target.
 * This allows objects to be reconstructed.  The RpcRequest is essentially the JSON message
 * coming from the web browser UI.
 */
public class HillviewComputation implements Serializable {
    static final long serialVersionUID = 1;

    private static final AtomicInteger currentId = new AtomicInteger(0);
    /**
     * Unique Id of this computation.
     */
    private final int id;
    /**
     * Request that triggered this computation.
     */
    public final RpcRequest request;

    /**
     * The id of the result produced by the computation.
     * Today all RpcRequests can produce at most one result.
     * We will have to revisit this architecture if this changes.
     */
    final RpcTarget.Id resultId;
    /**
     * Announce these guys when the object with resultId has been created.
     * This field should not be serialized.
     */
    private List<RpcTargetAction> onCreate;

    public HillviewComputation(@Nullable RpcTarget.Id resultId, RpcRequest request) {
        this.id = HillviewComputation.currentId.getAndIncrement();
        this.request = request;
        assert resultId != request.objectId;
        this.resultId = resultId == null ? RpcTarget.Id.freshId() : resultId;
        this.onCreate = new ArrayList<RpcTargetAction>();
    }

    private RpcTarget.Id getSourceId() {
        return this.request.objectId;
    }

    private synchronized void registerOnCreate(RpcTargetAction action) {
        // If the object already exists notify right away
        // Hopefully there is no race which could lose a notification this way.
        RpcTarget target = RpcObjectManager.instance.getObject(this.resultId);
        if (target != null) {
            action.action(target);
        } else {
            this.onCreate.add(action);
        }
    }

    synchronized void replay(RpcTargetAction action) {
        HillviewLogger.instance.info("Attempt to replay", "{0}", this);
        // Tell this guy when the produced object is created
        this.registerOnCreate(action);

        // This observer is notified when the source object for this
        // computation has been recreated.
        RpcTargetAction sourceAction = new RpcTargetAction(this.getSourceId()) {
            @Override
            public void action(RpcTarget source) {
                // Before executing the computation again check if the destination
                // object has not appeared.  There is a race between multiple
                // copies of a computation executing.
                if (RpcObjectManager.instance.getObject(HillviewComputation.this.resultId) != null) {
                    HillviewLogger.instance.info("Source retrieved but destination found",
                            "Dest={0}, Request={1}",
                            HillviewComputation.this.resultId,
                            HillviewComputation.this.request.toString());
                    return;
                }

                // Executing this function will probably create the
                // target object, but we don't know when exactly.
                HillviewLogger.instance.info("Source retrieved; invoking request",
                        "Source={0}, Request={1}",
                        source.toString(), HillviewComputation.this.request.toString());
                source.execute(HillviewComputation.this.request,
                        new RpcRequestContext(HillviewComputation.this));
            }
        };

        // Trigger the computation by retrieving the source; when that's done it will
        // start sourceNotify which will rerun this computation.
        RpcObjectManager.instance.executeAction(sourceAction);
    }

    @Override
    public String toString() {
        return "[Id=" + this.id +
                ", destination=" + this.resultId +
                ", RpcRequest=" + this.request + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        HillviewComputation that = (HillviewComputation) o;
        return this.request.equals(that.request) && this.resultId.equals(that.resultId);
    }

    @Override
    public int hashCode() {
        int result = this.request.hashCode();
        result = 31 * result + this.resultId.hashCode();
        return result;
    }

    /**
     * This is invoked when the object with resultId has finally been created
     * and inserted in the RpcObjectManager.
     */
    void objectCreated(RpcTarget target) {
        for (RpcTargetAction o: this.onCreate) {
            HillviewLogger.instance.info("Notifying observer of new object.",
                    "Computation={0}", this.toString());
            o.action(target);
        }
        this.onCreate.clear();
    }
}
