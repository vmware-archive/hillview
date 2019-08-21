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

package org.hillview.main;

import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.storage.OrcFileWriter;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.HillviewLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * This entry point is only used for preparing some data files for a demo.
 * It takes files named like data/ontime/On_Time_On_Time_Performance_*_*.csv and
 * removes some columns from them and converts them to the ORC format.
 */
class DemoDataCleaner {
    private static final String dataFolder = "../data/ontime";

    public static void main(String[] args) throws IOException {
        HillviewLogger.initialize("data cleaner", "hillview.log");
        String prefix = "On_Time_On_Time_Performance_";
        Path folder = Paths.get(dataFolder);
        Stream<Path> files = Files.walk(folder, 1);
        Schema schema = Schema.readFromJsonFile(Paths.get(dataFolder, "short.schema"));

        files.filter(f -> {
            String filename = f.getFileName().toString();
            if (!filename.contains("csv")) return false;
            //noinspection RedundantIfStatement
            if (!filename.startsWith(prefix)) return false;
            return true;
        }).sorted(Comparator.comparing(Path::toString))
                .forEach(f -> {
                    String filename = f.toString();
                    CsvFileLoader.Config config = new CsvFileLoader.Config();
                    config.allowFewerColumns = false;
                    config.hasHeaderRow = true;
                    CsvFileLoader r = new CsvFileLoader(filename, config, dataFolder + "/On_Time.schema");

                    System.out.println("Reading " + f);
                    ITable tbl = r.load();
                    ITable p = tbl.project(schema);

                    String end = filename.replace(prefix, "");
                    if (end.endsWith(".gz"))
                        // the output is uncompressed
                        end = end.replace(".gz", "");
                    if (!Files.exists(Paths.get(end))) {
                        CsvFileWriter writer = new CsvFileWriter(end);
                        System.out.println("Writing " + end);
                        writer.writeTable(p);
                    }

                    end = end.replace(".csv", ".orc");
                    OrcFileWriter owriter = new OrcFileWriter(end);
                    System.out.println("Writing " + end);
                    owriter.writeTable(p);

                    String big = filename.replace(".csv.gz", ".orc");
                    owriter = new OrcFileWriter(big);
                    System.out.println("Writing " + big);
                    owriter.writeTable(tbl);
                });
    }
}
