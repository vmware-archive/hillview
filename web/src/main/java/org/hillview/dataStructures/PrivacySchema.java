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
import org.hillview.table.columns.ColumnPrivacyMetadata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;

/**
 * PrivacySchema contains additional metadata for columns that are visualized using the binary mechanism for
 * differential privacy, as per Chan, Song, Shi, TISSEC '11 (https://eprint.iacr.org/2010/076.pdf).
 * Epsilon budgets can be specified for both single-column (1-d) queries as well as for multi-column queries.
 * Metadata for a multi-column histogram is indexed by the key corresponding to the concatenation of the column names,
 * in alphabetical order, with "+" as the delimiter.
 * */
public class PrivacySchema implements IJson {
    private HashMap<String, ColumnPrivacyMetadata> metadata;

    public PrivacySchema(HashMap<String, ColumnPrivacyMetadata> metadata) {
        this.metadata = metadata;
    }

    public ColumnPrivacyMetadata get(String colName) {
        return metadata.get(colName);
    }

    public ColumnPrivacyMetadata get(String[] colNames) {
        Arrays.sort(colNames);
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < colNames.length - 1; i++) {
            keyBuilder.append(colNames[i]);
            keyBuilder.append("+");
        }
        keyBuilder.append(colNames[colNames.length - 1]);
        String key = keyBuilder.toString();
        return metadata.get(key);
    }

    public static PrivacySchema loadFromString(String jsonString) {
        return IJson.gsonInstance.fromJson(jsonString, PrivacySchema.class);
    }

    /* One metadata object per column. */
    public static PrivacySchema loadFromFile(Path metadataFname) {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(metadataFname);
            contents = new String(encoded, StandardCharsets.US_ASCII);
        } catch ( java.io.IOException e ) {
            throw new RuntimeException(e);
        }

        return loadFromString(contents);
    }
}
