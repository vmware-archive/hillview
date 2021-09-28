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

package org.hillview.utils;

import com.google.gson.reflect.TypeToken;
import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Parse a Json string representing an object and return
 * all field as key-value strings.
 */
public class JsonFieldsExtractor implements IKVParsing {
    @Override
    public int parse(@Nullable String json, BiFunction<String, String, Boolean> consumer) {
        int fields = 0;
        if (json == null)
            return fields;
        Map<String, Object> map = IJson.gsonInstance.fromJson(json,
                new TypeToken<Map<String, String>>() {
                }.getType());
        if (map == null)
            return fields;
        for (Map.Entry<String, Object> e: map.entrySet()) {
            boolean done = consumer.apply(e.getKey(), e.getValue().toString());
            fields++;
            if (done) break;
        }
        return fields;
    }
}
