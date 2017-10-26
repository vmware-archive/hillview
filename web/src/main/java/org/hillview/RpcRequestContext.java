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

import javax.annotation.Nullable;
import javax.websocket.Session;

/**
 * This class models the context in which an RpcRequest is executed.
 */
public final class RpcRequestContext {
    /**
     * If non-null this request is being executed within the context of a web transaction
     * initiated by a user.
     */
    @Nullable
    final Session session;
    /**
     * If non-null this request is being executed when replaying the specified computation.
     */
    @Nullable
    final HillviewComputation computation;

    RpcRequestContext(Session session) {
        this.session = session;
        this.computation = null;
    }

    RpcRequestContext(HillviewComputation computation) {
        this.session = null;
        this.computation = computation;
    }

    /**
     * Get the current session if still open.
     * @return  The session if it exists and open, null otherwise.
     */
    @Nullable
    public Session getSessionIfOpen() {
        if (this.session == null)
            return null;
        if (!this.session.isOpen()) {
            HillviewLogger.instance.warn("Session already closed");
            return null;
        }
        return this.session;
    }
}
