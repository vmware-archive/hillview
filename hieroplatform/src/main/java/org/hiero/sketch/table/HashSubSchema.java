/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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
 *
 */

package org.hiero.sketch.table;
import org.hiero.sketch.table.api.ISubSchema;
import java.util.HashSet;

public class HashSubSchema implements ISubSchema {
    private final HashSet<String> colNames;

    public HashSubSchema() {
        this.colNames = new HashSet<String>();
    }

    public void add(final String newCol){
        this.colNames.add(newCol);
    }

    @Override
    public boolean isColumnPresent(final String name) {
        return this.colNames.contains(name);
    }
}
