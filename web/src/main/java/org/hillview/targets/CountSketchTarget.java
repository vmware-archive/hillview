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

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.RpcTarget;
import org.hillview.sketches.CountSketchResult;
import org.hillview.utils.HillviewLogger;

/**
 * This stores the result of running CountSketch in the first phase of the L_2 heavy hitters
 * computation. The CountSketchResult will then be used to invoke ExactCountSketch.
 * This object has no RPC methods per se, but it can be used as an argument for other RPC methods.
 */

public class CountSketchTarget extends RpcTarget {
    final CountSketchResult result;

    CountSketchTarget(final CountSketchResult result, final HillviewComputation computation) {
        super(computation);
        this.result = result;
        HillviewLogger.instance.info("Count Sketch");
        this.registerObject();
    }
}
