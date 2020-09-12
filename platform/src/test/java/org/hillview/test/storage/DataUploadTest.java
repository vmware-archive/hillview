/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.test.storage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.hillview.table.LazySchema;
import org.hillview.main.DataUpload;
import org.hillview.storage.OrcFileLoader;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

public class DataUploadTest extends BaseTest {
    @Test
    public void testChopCsv() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        String file = dataDir + "/ontime/2016_1.csv";
        int parts = upload.run("-f", file, "-l",
                "200000", "-h", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(3, parts);
    }

    private void deleteOrcChecksums(String file, int parts) {
        for (int i = 0; i < parts; i++) {
            String base = FilenameUtils.getBaseName(file);
            String ckname = "." + base + i + ".orc.crc";
            File f = new File(ckname);
            boolean success = f.delete();
            Assert.assertTrue(success);
        }
    }

    @Test
    public void testChopCsvSkip() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        String file = dataDir + "/ontime/2016_1.csv";
        int parts = upload.run("-f", file, "-l",
                "200000", "-o", "csv", "-h", "-d", dir.toString(), "--skip", "200000");
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(2, parts);
    }

    @Test
    public void testChopCsvToOrc() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        String file = dataDir + "/ontime/2016_1.csv";
        int parts = upload.run(
                "-f", file, "-o", "orc",
                "-l", "200000", "-h", "-d", dir.toString());

        OrcFileLoader loader = new OrcFileLoader(dir.toString() + "/2016_10.orc", new LazySchema(), false);
        ITable table = loader.load();
        Assert.assertNotNull(table);
        IColumn date = table.getLoadedColumn("FlightDate");
        Assert.assertNotNull(date);
        Assert.assertEquals(ContentsKind.LocalDate, date.getKind());
        LocalDateTime dt = (LocalDateTime)date.getObject(0);
        Assert.assertNotNull(dt);
        Assert.assertEquals(0, dt.getHour());

        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        this.deleteOrcChecksums(file, parts);
        Assert.assertEquals(3, parts);
    }

    @Test
    public void testChopLogToCsv() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run(
                "-f", dataDir + "/sample_logs/blockTracelog", "-p",
                "%{BLOCKTRACE}", "-o", "csv", "-l", "100", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(2, parts);
    }

    @Test
    public void testChopLogToCsvSkipToEmpty() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run(
                "-f", dataDir + "/sample_logs/blockTracelog", "-p",
                "%{BLOCKTRACE}", "-o", "csv", "-l", "100", "-d", dir.toString(), "--skip", "300");
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(1, parts);
    }

    @Test
    public void testChopLogToOrc() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        String file = dataDir + "/sample_logs/blockTracelog";
        int parts = upload.run("-f", file, "-o", "orc",
                "-p", "%{BLOCKTRACE}", "-l", "100", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        this.deleteOrcChecksums(file, parts);
        Assert.assertEquals(2, parts);
    }
}
