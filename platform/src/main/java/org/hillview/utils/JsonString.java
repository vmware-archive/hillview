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

package org.hillview.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.IJsonSketchResult;

import javax.annotation.Nullable;

/**
 * A string whose value is a JSON object.
 */
public class JsonString implements IJsonSketchResult {
    static final long serialVersionUID = 1;

    @Nullable
    private final String value;

    public JsonString(@Nullable String value) {
        this.value = value;
    }

    @Override
    public JsonElement toJsonTree() {
        if (this.value == null)
            return JsonNull.INSTANCE;
        return JsonParser.parseString(this.value);
    }
}
