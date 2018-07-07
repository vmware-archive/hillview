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
import org.hillview.table.Schema;

/**
 * This control message causes the remote servers to remove everything from their
 * memoization caches.
 */
public class PurgeMemoization extends ControlMessage {
    public Status remoteServerAction(HillviewServer server) {
        server.purgeMemoized();
        Schema.purgeCache();
        return new Status("caches purged");
    }
}
