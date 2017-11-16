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
import org.hillview.maps.LoadCsvFileMapper;
import org.hillview.storage.CsvFileObject;

// All RpcTarget objects must be public
@SuppressWarnings("WeakerAccess")
public final class CsvFileTarget extends RpcTarget {
    private final IDataSet<CsvFileObject> files;

    CsvFileTarget(IDataSet<CsvFileObject> files, HillviewComputation computation) {
        super(computation);
        this.files = files;
        this.registerObject();
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        this.runMap(this.files, new LoadCsvFileMapper(), TableTarget::new, request, context);
    }
}
