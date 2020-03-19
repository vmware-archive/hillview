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

import java.io.Serializable;

import org.hillview.dataset.api.ISketch;

/**
 * Wrap an ISketch object to be sent to a remote node
 * @param <T> Input type of the sketch function
 * @param <R> Output type of the sketch function
 */
public class SketchOperation<T, R extends Serializable> extends RemoteOperation {
    static final long serialVersionUID = 1;

    public final ISketch<T, R> sketch;

    public SketchOperation(final ISketch<T, R> sketch) {
        this.sketch = sketch;
    }
}
