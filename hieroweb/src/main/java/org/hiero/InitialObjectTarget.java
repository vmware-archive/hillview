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

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;

import javax.websocket.Session;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Stream;

public class InitialObjectTarget extends RpcTarget {
    @HieroRpc
    void prepareFiles(RpcRequest request, Session session) throws IOException {
        // TODO: look at request.  Now we just supply always the same table
        String dataFolder = "/home/mbudiu/git/hiero/data";
        String schemaFile = "short.schema";
        Path schemaPath = Paths.get(dataFolder, schemaFile);

        /*
        List<IDataSet<CsvFileObject>> fileNames = new ArrayList<IDataSet<CsvFileObject>>();

        Path folder = Paths.get(dataFolder);
        Stream<Path> files = Files.walk(folder, 1);
        files.filter(f -> {
            String filename = f.getFileName().toString();
            if (!filename.endsWith(".csv")) return false;
            if (!filename.startsWith("2016")) return false;
            return true;
        }).forEach(f -> {
            CsvFileObject cfo = new CsvFileObject(f, schemaPath);
            LocalDataSet<CsvFileObject> local = new LocalDataSet<CsvFileObject>(cfo);
            fileNames.add(local);
            logger.log(Level.INFO, "Added " + toString());
        });

        ParallelDataSet<CsvFileObject> result = new ParallelDataSet<CsvFileObject>(fileNames);
        */
        Path f = Paths.get(dataFolder, "2016_1.csv");
        CsvFileObject cfo = new CsvFileObject(f, schemaPath);
        LocalDataSet<CsvFileObject> result = new LocalDataSet<CsvFileObject>(cfo);

        FileNamesTarget target = new FileNamesTarget(result);
        RpcReply reply = request.createReply(target.idToJson());
        reply.send(session);
        request.syncCloseSession(session);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }
}
