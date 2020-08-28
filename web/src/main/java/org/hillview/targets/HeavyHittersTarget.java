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

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.RpcTarget;
import org.hillview.sketches.results.FreqKList;
import org.hillview.utils.HillviewLogger;

/**
 * This object has no RPC methods per se, but it can be used
 * as an argument for other RPC methods.
 */
final class HeavyHittersTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    final FreqKList heavyHitters;

    HeavyHittersTarget(final FreqKList heavyHitters, final HillviewComputation computation) {
        super(computation, null);
        this.heavyHitters = heavyHitters;
        HillviewLogger.instance.info("Heavy hitters", "{0}", heavyHitters.getSize());
        this.registerObject();
    }
}
