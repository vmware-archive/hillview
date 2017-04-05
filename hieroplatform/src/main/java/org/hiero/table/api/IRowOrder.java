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

public interface IRowOrder {
    /**
     * @return Total number of rows.
     */
    int getSize();

    /**
     * @return An iterator over all the rows in the membership map.
     * The iterator is initialized to point at the "first" row.
     * The iterator is deterministic, multiple invocations result in an iterator that gives the
     * same row ordering.
     */
    IRowIterator getIterator();
}
