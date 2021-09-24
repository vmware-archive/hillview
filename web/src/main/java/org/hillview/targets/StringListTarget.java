/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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
import org.hillview.sketches.DistinctStrings;
import org.hillview.utils.HillviewLogger;

/**
 * This class does not have any interesting methods; it just stores inside a list of strings.
 */
public class StringListTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    final DistinctStrings strings;

    StringListTarget(final DistinctStrings strings, final HillviewComputation computation) {
        super(computation, null);
        this.strings = strings;
        HillviewLogger.instance.info("String List", "{0}", this.strings.size());
        this.registerObject();
    }
}
