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
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import rx.Observer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents information that has led to the creation of an RpcTarget object.
 * The information stores the inputs and the request that cause the creation of the target.
 * This allows objects to be reconstructed.  The RpcRequest is essentially the JSON message
 * coming from the web browser UI.
 */
public class HillviewComputation implements Serializable {
    /**
     * Request that triggered this computation.
     */
    private final RpcRequest request;
    /**
     * The id of the target on which the request was executed.
     */
    private final String sourceId;
    /**
     * The id of the result produced by the computation.
     * Today all RpcRequests can produce at most one result.
     * We will have to revisit this architecture if this changes.
     */
    final String resultId;
    /**
     * Announce these guys when the object with resultId has been created.
     * This field should not be serialized.
     */
    private List<Observer<RpcTarget>> onCreate;

    HillviewComputation(RpcTarget source, RpcRequest request) {
        this.request = request;
        this.sourceId = Converters.checkNull(source.objectId);
        this.resultId = this.freshId();
        this.onCreate = new ArrayList<Observer<RpcTarget>>();
    }

    private synchronized void registerOnCreate(Observer<RpcTarget> toNotify) {
        // If the object already exists notify right away
        // Hopefully there is no race which could lose a notification this way.
        RpcTarget target = RpcObjectManager.instance.getObject(this.resultId);
        if (target != null) {
            toNotify.onNext(target);
            toNotify.onCompleted();
        } else {
            this.onCreate.add(toNotify);
        }
    }

    /**
     * Allocate a fresh identifier.
     */
    private String freshId() {
        return UUID.randomUUID().toString();
    }

    void replay(Observer<RpcTarget> toNotify) {
        HillviewLogger.instance.info("Attempt to replay", "{0}", this);
        // This observer is notified when the source object has been recreated.
        Observer<RpcTarget> sourceNotify = new SingleObserver<RpcTarget>() {
            @Override
            public void onError(Throwable throwable) {
                toNotify.onError(throwable);
            }

            @Override
            public void onSuccess(RpcTarget source) {
                // Executing this function will probably create the
                // target object, but we don't know when exactly.
                source.execute(HillviewComputation.this.request,
                        new RpcRequestContext(HillviewComputation.this));
            }
        };

        // Tell this guy when the destination is done
        this.registerOnCreate(toNotify);
        // Trigger the computation by retrieving the source
        RpcObjectManager.instance.retrieveTarget(this.sourceId, true, sourceNotify);
    }

    @Override
    public String toString() {
        return "Source: " + this.sourceId + ", request: " + this.request.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        HillviewComputation that = (HillviewComputation) o;
        return this.request.equals(that.request) && this.sourceId.equals(that.sourceId);
    }

    @Override
    public int hashCode() {
        int result = this.request.hashCode();
        result = 31 * result + this.sourceId.hashCode();
        return result;
    }

    /**
     * This is invoked when the object with resultId has finally been created
     * and inserted in the RpcObjectManager.
     */
    void objectCreated(RpcTarget target) {
        for (Observer<RpcTarget> o: this.onCreate) {
            o.onNext(target);
            o.onCompleted();
        }
        this.onCreate.clear();
    }
}
