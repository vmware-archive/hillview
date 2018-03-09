/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

package org.hillview;

import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.table.HashSubSchema;
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
 * TODO: delete this class
 * This entry point is only used for preparing some data files for a demo.
 * It takes files named like data/ontime/On_Time_On_Time_Performance_*_*.csv and
 * removes some columns from them.  Optionally, it can also split these into
 * smaller files each.
 */
public class DemoDataCleaner {
    static final String dataFolder = "../data/ontime";
    static final String schemaFile = "On_Time.schema";

    public static void main(String[] args) throws IOException {
        HillviewLogger.initialize("data cleaner", "hillview.log");
        String[] columns = {
                "DayOfWeek", "FlightDate", "UniqueCarrier",
                "Origin", "OriginCityName", "OriginState", "Dest", "DestState",
                "DepTime", "DepDelay", "ArrTime", "ArrDelay", "Cancelled",
                "ActualElapsedTime", "Distance"
        };

        System.out.println("Splitting files in folder " + dataFolder);
        Path schemaPath = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(schemaPath);
        HashSubSchema subSchema = new HashSubSchema(columns);
        Schema proj = schema.project(subSchema);
        proj.writeToJsonFile(Paths.get(dataFolder, "short.schema"));

        String prefix = "On_Time_On_Time_Performance_";
        Path folder = Paths.get(dataFolder);
        Stream<Path> files = Files.walk(folder, 1);
        files.filter(f -> {
            String filename = f.getFileName().toString();
            if (!filename.endsWith("csv")) return false;
            //noinspection RedundantIfStatement
            if (!filename.startsWith(prefix)) return false;
            return true;
        }).sorted(Comparator.comparing(Path::toString))
                .forEach(f -> {
                    String filename = f.toString();
                    CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
                    config.allowFewerColumns = false;
                    config.hasHeaderRow = true;
                    CsvFileLoader r = new CsvFileLoader(filename, config, schemaPath.toString());

                    System.out.println("Reading " + f);
                    ITable tbl = r.load();
                    ITable p = tbl.project(proj);

                    String end = filename.replace(prefix, "");
                    CsvFileWriter writer = new CsvFileWriter(end);
                    try {
                        System.out.println("Writing " + end);
                        writer.writeTable(p);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }
}
