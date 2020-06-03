/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketchResult;

import java.util.Objects;

/**
 * This is a version of Group which is serializable as JSON.
 * @param <R>  Type of data that is grouped.
 */
public class JsonGroups<R extends IJson> implements IJson {
    /**
     * For each bucket one result.
     */
    public final JsonList<R> perBucket;
    /**
     * For the bucket corresponding to the 'missing' value on result.
     */
    public final R           perMissing;

    public JsonGroups(JsonList<R> perBucket, R perMissing) {
        this.perBucket = perBucket;
        this.perMissing = perMissing;
    }
}
