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

package org.hillview.table.api;

import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public interface IStringColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter conv) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final String tmp = this.getString(rowIndex);
        return Converters.checkNull(conv).asDouble(Converters.checkNull(tmp));
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        return this.getString(rowIndex);
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IStringColumn.this.isMissing(i);
                final boolean jMissing = IStringColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Converters.checkNull(IStringColumn.this.getString(i)).compareTo(
                            Converters.checkNull(IStringColumn.this.getString(j)));
                }
            }
        };
    }
}