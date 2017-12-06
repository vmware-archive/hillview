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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.storage.JsonFileReader;
import org.hillview.table.api.ITable;

import java.nio.file.Paths;

/**
 * Reads the specified file assuming it contains a single JSON object.
 * See the JsonFileReader class for a description of the expected file format.
 */
public class LoadJsonFileMapper implements IMap<String, ITable> {
    @Override
    public ITable apply(String data) {
        JsonFileReader reader = new JsonFileReader(Paths.get(data));
        return reader.read();
    }
}
