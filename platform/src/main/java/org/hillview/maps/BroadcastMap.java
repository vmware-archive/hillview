/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;

/**
 * A BroadcastMap is a mapper which broadcasts a value of type S to all
 * leaves of an IDataSet and creates an IDataSet[S] having the exact
 * same value in all leaves.
 * @param <T> Type of original dataset; unused.
 * @param <S> Type of data broadcast.
 */
public class BroadcastMap<T, S> implements IMap<T, S> {
    private final S data;

    public BroadcastMap(S data) {
        this.data = data;
    }

    @Override
    public S apply(T unused) {
        return this.data;
    }
}
