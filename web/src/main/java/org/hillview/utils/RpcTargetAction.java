/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.RpcTarget;

/**
 * A pending operation on an RpcTarget object.  The object may no longer exists,
 * and then it will need
 * to be recreated.  The action method will be executed on the
 * target when the target is found or recreated.
 */
public abstract class RpcTargetAction {
    public final RpcTarget.Id id;

    public RpcTargetAction(RpcTarget.Id id) {
        this.id = id;
    }

    /**
     * Action to execute when the target has been obtained.
     * @param target  Actual RpcTarget.
     */
    public abstract void action(RpcTarget target);
}
