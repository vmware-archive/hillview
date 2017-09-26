/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LoadFileMapper;
import org.hillview.utils.CsvFileObject;

import javax.websocket.Session;

public class FileNamesTarget extends RpcTarget {
    private final IDataSet<CsvFileObject> files;

    FileNamesTarget(IDataSet<CsvFileObject> files) {
        this.files = files;
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, Session session) {
        this.runMap(this.files, new LoadFileMapper(), TableTarget::new, request, session);
    }

    @Override
    public String toString() {
        return "FileNamesTarget object, " + super.toString();
    }
}
