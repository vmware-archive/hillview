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
 *
 */

package org.hiero.table.api;

import org.hiero.utils.Converters;
import javax.annotation.Nullable;
import java.time.Duration;

public interface IDurationColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final Duration tmp = this.getDuration(rowIndex);
        return Converters.toDouble(Converters.checkNull(tmp));
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Converters.checkNull(this.getDuration(rowIndex)).toString();
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IDurationColumn.this.isMissing(i);
                final boolean jMissing = IDurationColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Converters.checkNull(IDurationColumn.this.getDuration(i)).
                            compareTo(Converters.checkNull(IDurationColumn.this.getDuration(j)));
                }
            }
        };
    }
}
