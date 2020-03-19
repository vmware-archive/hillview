/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.NextKList;

/**
 * The result of heavyHitterSketch.
 */
public class TopList implements IJson {
    static final long serialVersionUID = 1;

    /**
     * The NextKList stores the fields to display and their counts.
     */
    public final NextKList top;
    /**
     * The id of the FreqKList object which might be used for further filtering.
     */
    public final String heavyHittersId;

    public TopList(NextKList top, String heavyHittersId) {
        this.top = top;
        this.heavyHittersId = heavyHittersId;
    }
}
