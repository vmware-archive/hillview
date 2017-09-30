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

package org.hillview.dataset.remoting;

import org.hillview.dataset.api.ControlMessage;
import java.io.Serializable;

/**
 * Wrap a ControlMessage object to be sent to a remote node
 */
public class ManageOperation extends RemoteOperation implements Serializable {
    public final ControlMessage message;
    public ManageOperation(final ControlMessage message) {
        this.message = message;
    }
}
