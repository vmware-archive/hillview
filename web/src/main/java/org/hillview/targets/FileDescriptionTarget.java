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
import org.hillview.maps.FalseMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.FileSizeSketch;
import org.hillview.storage.IFileReference;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * This is an RpcTarget object which stores a file loader name in each leaf.
 */
public class FileDescriptionTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    protected final IDataSet<IFileReference> files;

    FileDescriptionTarget(IDataSet<IFileReference> files, HillviewComputation computation,
                          @Nullable String metadataDirectory) {
        super(computation, metadataDirectory);
        this.files = files;
        this.registerObject();
    }

    @HillviewRpc
    public void getFileSize(RpcRequest request, RpcRequestContext context) {
        FileSizeSketch sk = new FileSizeSketch();
        this.runSketch(this.files, sk, request, context);
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        this.runMap(this.files, loader, (d, c) -> new TableTarget(d, c, this.metadataDirectory), request, context);
    }

    @HillviewRpc
    public void prune(RpcRequest request, RpcRequestContext context) {
        this.runPrune(this.files, new FalseMap<IFileReference>(),
                (d, c) -> new FileDescriptionTarget(d, c, this.metadataDirectory), request, context);
    }
}
