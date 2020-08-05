/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.management;

import org.hillview.dataset.api.ControlMessage;
import org.hillview.dataset.remoting.HillviewServer;

/**
 * This control message causes the servers to change the way they memoize
 * results of computations.
 */
public class SetMemoization extends ControlMessage {
    static final long serialVersionUID = 1;
    private final boolean state;

    public SetMemoization(boolean state) {
        this.state = state;
    }

    public Status remoteServerAction(HillviewServer server) {
        server.setMemoization(this.state);
        return new Status("OK");
    }
}
