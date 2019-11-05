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

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes an aggregation operation to be performed.
 * All aggregates ignore null values.
 */
public class AggregateDescription implements Serializable {
    public enum AggregateKind implements Serializable {
       Sum,
       Count,
       Min,
       Max,
       Average
    }

    /**
     * Column which is aggregated.
     */
    public final ColumnDescription cd;
    public final AggregateKind agkind;
    /**
     * The internal flag is set for aggregates that are not requested by
     * the user, but generated internally.  For example, computing Average
     * requires computing a Sum and Count.
     */
    public boolean internal;

    public AggregateDescription(ColumnDescription cd, AggregateKind agkind) {
        this(cd, agkind, false);
    }

    public AggregateDescription(ColumnDescription cd, AggregateKind agkind, boolean internal) {
        this.cd = cd;
        this.agkind = agkind;
        this.internal = internal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateDescription that = (AggregateDescription) o;
        return internal == that.internal &&
                cd.equals(that.cd) &&
                agkind == that.agkind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cd, agkind, internal);
    }

    @Override
    public String toString() {
        return this.cd.toString() + ":" + this.agkind.toString();
    }
}
