/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hiero;

import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.IMap;
import org.hiero.sketch.table.api.ITable;

import javax.websocket.Session;
import java.io.IOException;
import java.util.function.Function;
import java.util.logging.Level;

public class FileNamesTarget extends RpcTarget {
    private final IDataSet<CsvFileObject> files;

    class LoadFileMapper implements IMap<CsvFileObject, ITable> {
        @Override
        public ITable apply(CsvFileObject csvFileObject) {
            try {
                logger.log(Level.INFO, "Loading " + csvFileObject);
                return csvFileObject.loadTable();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    FileNamesTarget(IDataSet<CsvFileObject> files) {
        this.files = files;
    }

    @HieroRpc
    public void loadTable(RpcRequest request, Session session) {
        LoadFileMapper mapper = new LoadFileMapper();
        Function<IDataSet<ITable>, RpcTarget> factory = TableTarget::new;
        this.runMap(this.files, mapper, factory, request, session);
    }

    @Override
    public String toString() {
        return "FileNamesTarget object, " + super.toString();
    }
}
