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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

    @Nullable
    public static AggregateDescription[] getAggregates(@Nullable AggregateDescription[] aggregates) {
        if (aggregates == null)
            return null;

        // Replaces Average with a Sum and Count
        List<AggregateDescription> result = new ArrayList<AggregateDescription>();
        for (AggregateDescription a: aggregates) {
            if (a.agkind == AggregateDescription.AggregateKind.Average) {
                // Search a sum and a count on the same column
                boolean sumFound = false;
                boolean countFound = false;
                for (AggregateDescription o: aggregates) {
                    if (!o.cd.equals(a.cd))
                        continue;
                    if (o.agkind == AggregateDescription.AggregateKind.Sum)
                        sumFound = true;
                    else if (o.agkind == AggregateDescription.AggregateKind.Count)
                        countFound = true;
                }
                if (!sumFound)
                    result.add(new AggregateDescription(
                            a.cd, AggregateDescription.AggregateKind.Sum, true));
                if (!countFound)
                    result.add(new AggregateDescription(
                            a.cd, AggregateDescription.AggregateKind.Count, true));
            } else {
                result.add(a);
            }
        }
        return result.toArray(new AggregateDescription[0]);
    }
}
