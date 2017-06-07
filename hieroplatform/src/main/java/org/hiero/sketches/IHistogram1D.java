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

package org.hiero.sketches;

import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IStringConverter;

import javax.annotation.Nullable;

public interface IHistogram1D {
    void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                               @Nullable final IStringConverter converter, double sampleRate);

    void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                               @Nullable final IStringConverter converter, double sampleRate, long seed);

    void createHistogram(final IColumn column, final IMembershipSet membershipSet,
                         @Nullable final IStringConverter converter);

    int getNumOfBuckets();
}
