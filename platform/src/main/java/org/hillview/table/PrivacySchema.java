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

package org.hillview.table;

import org.hillview.dataset.api.IJson;
import org.hillview.table.columns.ColumnQuantization;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * PrivacySchema contains additional metadata for columns that are visualized using the binary mechanism for
 * differential privacy, as per Chan, Song, Shi, TISSEC '11 (https://eprint.iacr.org/2010/076.pdf).
 * Epsilon budgets can be specified for both single-column (1-d) queries as well as for multi-column queries.
 * Metadata for a multi-column histogram is indexed by the key corresponding to the concatenation of the column names,
 * in alphabetical order, with "+" as the delimiter.
 * */
public class PrivacySchema implements IJson, Serializable {
    final public QuantizationSchema quantization;
    // We use a LinkedHashMap for deterministic serialization
    final private LinkedHashMap<String, Double> epsilons;

    public PrivacySchema(QuantizationSchema quantization) {
        this.quantization = quantization;
        this.epsilons = new LinkedHashMap<String, Double>();
    }

    @Nullable
    public ColumnQuantization quantization(String colName) {
        return this.quantization.get(colName);
    }

    /**
     * Get the epsilon corresponding to a set of columns.
     * @param colNames  Columns that we need the epsilon for.
     * @return          epsilon exploring the joint columns.
     *                  0 if there is no epsilon for this group of columns.
     */
    public double epsilon(String[] colNames) {
        Arrays.sort(colNames);
        String key = String.join("+", colNames);
        return this.epsilon(key);
    }

    public double epsilon(String colName) {
        Double epsilon = this.epsilons.get(colName);
        if (epsilon == null)
            return 0.0;
        return epsilon;
    }

    public void setEpsilon(String colName, double epsilon) {
        this.epsilons.put(colName, epsilon);
    }

    public void setEpsilon(String[] colNames, double epsilon) {
        Arrays.sort(colNames);
        String key = String.join("+", colNames);
        this.setEpsilon(key, epsilon);
    }

    public static PrivacySchema loadFromString(String jsonString) {
        return IJson.gsonInstance.fromJson(jsonString, PrivacySchema.class);
    }

    public static PrivacySchema loadFromFile(String metadataFname) {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(metadataFname));
            contents = new String(encoded, StandardCharsets.US_ASCII);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
        return loadFromString(contents);
    }
}
