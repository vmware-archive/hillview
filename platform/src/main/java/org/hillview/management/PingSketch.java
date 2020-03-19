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

import org.hillview.dataset.api.ISketch;
import org.hillview.utils.JsonList;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.HashSet;

/**
 * Returns the names of the machines where the leaf data is.
 * Duplicate machine names are filtered.
 * @param <T>  Unused.
 */
public class PingSketch<T> implements ISketch<T, JsonList<String>> {
    static final long serialVersionUID = 1;
    @Nullable
    @Override
    public JsonList<String> zero() {
        return new JsonList<String>();
    }

    @Nullable
    @Override
    public JsonList<String> add(@Nullable JsonList<String> left, @Nullable JsonList<String> right) {
        HashSet<String> added = new HashSet<String>();
        JsonList<String> result = new JsonList<String>();
        assert left != null;
        assert right != null;
        for (String s : left)
            if (added.add(s))
                result.add(s);
        for (String s : right)
            if (added.add(s))
                result.add(s);
        return result;
    }

    @Nullable
    @Override
    public JsonList<String> create(@Nullable T data) {
        JsonList<String> result = new JsonList<String>(1);
        String host = Utilities.getHostName();
        result.add(host);
        return result;
    }
}
