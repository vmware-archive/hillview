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

import org.hiero.dataset.LocalDataSet;
import org.hiero.dataset.ParallelDataSet;
import org.hiero.dataset.api.IDataSet;

import javax.websocket.Session;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Stream;

public class InitialObjectTarget extends RpcTarget {
    @HieroRpc
    void prepareFiles(RpcRequest request, Session session) throws IOException {
        Path currentRelativePath = Paths.get("");
        String cwd = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current directory is: " + cwd);

        int which = request.parseArgs(Integer.class);

        String dataFolder = "../data/";
        String smallFileSchema = "On_Time.schema";
        String smallFile = "On_Time_Sample.csv";
        String schemaFile = "short.schema";
        Path schemaPath = Paths.get(dataFolder, schemaFile);

        IDataSet<CsvFileObject> result;
        if (which >= 0 && which <= 1) {
            int limit = which == 0 ? 3 : 1;
            List<IDataSet<CsvFileObject>> fileNames = new ArrayList<IDataSet<CsvFileObject>>();
            Path folder = Paths.get(dataFolder);
            Stream<Path> files = Files.walk(folder, 1, FileVisitOption.FOLLOW_LINKS);
            files.filter(f -> {
                String filename = f.getFileName().toString();
                if (!filename.endsWith(".csv")) return false;
                if (!filename.startsWith("2016")) return false;
                return true;
            }).limit(limit)
                    .sorted(Comparator.comparing(Path::toString))
                    .forEach(f -> {
                        CsvFileObject cfo = new CsvFileObject(f, schemaPath);
                        LocalDataSet<CsvFileObject> local = new LocalDataSet<CsvFileObject>(cfo);
                        fileNames.add(local);
                        logger.log(Level.INFO, "Added " + toString());
                    });
            if (fileNames.size() == 0) {
                RpcReply reply = request.createReply(new RuntimeException("No such files"));
                reply.send(session);
                request.syncCloseSession(session);
                return;
            }
            result = new ParallelDataSet<CsvFileObject>(fileNames);
        } else {
            CsvFileObject file = new CsvFileObject(
                    Paths.get(dataFolder, smallFile),
                    Paths.get(dataFolder, smallFileSchema));
            result = new LocalDataSet<CsvFileObject>(file);
        }
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
