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

package org.hillview;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
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
    @Nullable
    private final IDataSet<Empty> emptyDataset;

    InitialObjectTarget() {
        Empty empty = new Empty();
        this.emptyDataset = new LocalDataSet<Empty>(empty);
    }

    @SuppressWarnings("RedundantIfStatement")
    @HillviewRpc
    void prepareFiles(RpcRequest request, Session session) {
        int which = request.parseArgs(Integer.class);

        IMap<Empty, List<CsvFileObject>> mapper = (IMap<Empty, List<CsvFileObject>>) unused -> {
            Path currentRelativePath = Paths.get("");
            String cwd = currentRelativePath.toAbsolutePath().toString();
            System.out.println("Current directory is: " + cwd);

            String dataFolder = "../data/";
            String smallFileSchema = "On_Time.schema";
            String smallFile = "On_Time_Sample.csv";
            String schemaFile = "short.schema";

            String vropsFile = "vrops.csv";
            String vropsSchema = "vrops.schema";

            Path schemaPath = Paths.get(dataFolder, schemaFile);

            final List<CsvFileObject> result = new ArrayList<CsvFileObject>();
            if (which >= 0 && which <= 1) {
                int limit = which == 0 ? 3 : 1;
                Path folder = Paths.get(dataFolder);
                Stream<Path> files;
                try {
                    files = Files.walk(folder, 1, FileVisitOption.FOLLOW_LINKS);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                files.filter(f -> {
                    String filename = f.getFileName().toString();
                    if (!filename.endsWith(".csv")) return false;
                    if (!filename.startsWith("2016")) return false;
                    return true;
                }).limit(limit)
                        .sorted(Comparator.comparing(Path::toString))
                        .forEach(f -> {
                            CsvFileObject cfo = new CsvFileObject(f, schemaPath);
                            result.add(cfo);
                            logger.log(Level.INFO, "Added " + toString());
                        });
                if (result.size() == 0)
                    throw new RuntimeException("No such files");
            } else if (which == 2) {
                CsvFileObject file = new CsvFileObject(
                        Paths.get(dataFolder, smallFile),
                        Paths.get(dataFolder, smallFileSchema));
                result.add(file);
            } else if (which == 3) {
                CsvFileObject file = new CsvFileObject(
                        Paths.get(dataFolder, vropsFile),
                        Paths.get(dataFolder, vropsSchema));
                result.add(file);
            } else {
                throw new RuntimeException("Unexpected file");
            }
            return result;
        };

        Converters.checkNull(this.emptyDataset);
        this.runFlatMap(this.emptyDataset, mapper, FileNamesTarget::new, request, session);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }
}
