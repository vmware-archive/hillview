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

package org.hillview.test;

import org.hillview.storage.HillviewLogs;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LogFileParserTest extends BaseTest {
    @Test
    public void parseLogLines() throws IOException {
        String logFileContents = String.join("\n",
                "2017-10-03 13:41:52.931 [main] INFO Hillview - ubuntu,org.hillview.dataset" +
                        ".LocalDataSet,<clinit>,Detect CPUs,Using 3 processors",
                "2017-10-03 13:41:52.937 [main] INFO Hillview - ubuntu,org.hillview.dataset" +
                        ".ParallelDataSet,sketch,Invoked sketch,target=ParallelDataSet of size 3",
                "2017-10-03 13:41:53.024 [pool-1-thread-2] INFO Hillview - ubuntu,org.hillview" +
                        ".dataset.LocalDataSet,lambda$sketch$4,Starting sketch,org.hillview.sketches.SampleQuantileSketch",
                "2017-10-03 13:41:53.024 [pool-1-thread-1] INFO Hillview - ubuntu,org.hillview" +
                        ".dataset.LocalDataSet,lambda$sketch$4,Starting sketch,org.hillview.sketches.SampleQuantileSketch",
                "2017-10-03 13:41:53.032 [pool-1-thread-3] INFO Hillview - ubuntu,org.hillview" +
                        ".dataset.LocalDataSet,lambda$sketch$4,Starting sketch,org.hillview.sketches.SampleQuantileSketch"
        );

        File f = File.createTempFile("tmp", null, new File("."));
        f.deleteOnExit();
        PrintWriter out = new PrintWriter(f.getName());
        out.println(logFileContents);
        out.close();

        Path path = Paths.get(".", f.getName());
        ITable table = HillviewLogs.parseLogFile(path);
        Converters.checkNull(table);
        Assert.assertEquals(table.toString(), "Table[9x5]");
    }
}
