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

package org.hillview.storage;

import org.hillview.table.api.ITable;

/**
 * This class holds a reference to a "file" from some external storage medium.
 * This class can read the data in the file into an ITable using the load method.
 * Instances of this class are not serializable; they are all created where the data resides.
 */
public interface IFileReference {
    /**
     * Read the file, return a table.
     */
    ITable load();

    /**
     * The size of the file in bytes.
     */
    long getSizeInBytes();
}
