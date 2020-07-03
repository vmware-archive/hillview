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

import org.hillview.dataset.api.IMap;
import org.hillview.utils.Pair;

/**
 * Message type to initiate a zip command against two RemoteDataSets
 */
public class ZipOperation<T, S, R> extends RemoteOperation {
    static final long serialVersionUID = 1;
    public final int datasetIndex;

    public final IMap<Pair<T, S>, R> map;

    public ZipOperation(final int datasetIndex, IMap<Pair<T, S>, R> map) {
        this.map = map;
        this.datasetIndex = datasetIndex;
    }
}