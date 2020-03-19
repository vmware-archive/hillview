/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.storage.IFileReference;
import org.hillview.table.FileSizeInfo;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class FileSizeSketch implements ISketch<IFileReference, FileSizeInfo> {
    static final long serialVersionUID = 1;
    @Override
    public FileSizeInfo create(@Nullable IFileReference data) {
        Converters.checkNull(data);
        return new FileSizeInfo(1, data.getSizeInBytes());
    }

    @Nullable
    @Override
    public FileSizeInfo zero() {
        return new FileSizeInfo();
    }

    @Nullable
    @Override
    public FileSizeInfo add(@Nullable FileSizeInfo left, @Nullable FileSizeInfo right) {
        assert left != null;
        assert right != null;
        return new FileSizeInfo(left.fileCount + right.fileCount, left.totalSize + right.totalSize);
    }
}
