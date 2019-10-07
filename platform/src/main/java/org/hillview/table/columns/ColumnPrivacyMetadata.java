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

package org.hillview.table.columns;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;

/**
 * This class represents metadata used for computing differentially-private mechanisms.
 */
public class ColumnPrivacyMetadata implements IJson {
    /**
     * Total privacy budget allotted to this column.
     */
    public double epsilon;

    public ColumnPrivacyMetadata(double epsilon) {
        this.epsilon = epsilon;
        if (epsilon < 0)
            throw new IllegalArgumentException("Epsilon must be positive:" + epsilon);
    }

    // We expect only one of the following to be implemented
    public double roundDown(double value) {
        throw new UnsupportedOperationException();
    }
    @Nullable
    public String roundDown(@Nullable String value) {
        throw new UnsupportedOperationException();
    }
    public int roundDown(int value) {
        throw new UnsupportedOperationException();
    }
}
