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
import org.hillview.utils.HillviewLogging;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * Base interface for all computations.
 */
public class HillviewComputation implements Serializable {
    /**
     * Request that triggered this computation.
     */
    private final RpcRequest request;
    /**
     * The id of the targetId on which the request was executed.
     */
    private final String targetId;

    HillviewComputation(RpcTarget targetId, RpcRequest request) {
        this.request = request;
        this.targetId = Converters.checkNull(targetId.objectId);
    }

    void replay() {
        HillviewLogging.logger().info("Attempt to replay {}", this);
        try {
            RpcTarget target = RpcObjectManager.instance.retrieveTarget(this.targetId, true);
            target.execute(this.request, null);
            HillviewLogging.logger().info("Replay successful {}", this);
        } catch (InvocationTargetException|IllegalAccessException e) {
            HillviewLogging.logger().error("Exception while replaying {}", this.toString(), e);
        }
    }

    @Override
    public String toString() {
        return "Target: " + this.targetId + ", request: " + this.request.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        HillviewComputation that = (HillviewComputation) o;
        return this.request.equals(that.request) && this.targetId.equals(that.targetId);
    }

    @Override
    public int hashCode() {
        int result = this.request.hashCode();
        result = 31 * result + this.targetId.hashCode();
        return result;
    }
}
