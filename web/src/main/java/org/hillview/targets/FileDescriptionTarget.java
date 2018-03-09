/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.LoadFilesMapper;
import org.hillview.storage.IFileLoader;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * This is an RpcTarget object which stores a file loader name in each leaf.
 */
// All RpcTarget objects must be public
@SuppressWarnings("WeakerAccess")
public class FileDescriptionTarget extends RpcTarget {
    private final IDataSet<IFileLoader> files;

    public FileDescriptionTarget(IDataSet<IFileLoader> files, HillviewComputation computation) {
        super(computation);
        this.files = files;
        this.registerObject();
    }

    public static class SchemaFileLocation {
        @Nullable
        String schemaFilename;
        boolean headerRow;
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        IMap<IFileLoader, ITable> loader = new LoadFilesMapper();
        this.runMap(this.files, loader, TableTarget::new, request, context);
    }
}

