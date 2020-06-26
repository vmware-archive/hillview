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
import org.hillview.main.DataUpload;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataUploadTest extends BaseTest {
    @Test
    public void testChopCsv() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run("-f", "../data/ontime/2016_1.csv", "-l",
                "200000", "-h", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(3, parts);
    }

    @Test
    public void testChopCsvSkip() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run("-f", "../data/ontime/2016_1.csv", "-l",
                "200000", "-h", "-d", dir.toString(), "--skip", "200000");
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(2, parts);
    }

    @Test
    public void testChopCsvToOrc() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run(
                "-f", "../data/ontime/2016_1.csv", "-o",
                "-l", "200000", "-h", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(3, parts);
    }

    @Test
    public void testChopLogToCsv() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run(
                "-f", "../data/sample_logs/blockTracelog", "-p",
                "%{BLOCKTRACE}", "-l", "100", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(2, parts);
    }

    @Test
    public void testChopLogToCsvSkipToEmpty() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run(
                "-f", "../data/sample_logs/blockTracelog", "-p",
                "%{BLOCKTRACE}", "-l", "100", "-d", dir.toString(), "--skip", "300");
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(1, parts);
    }

    @Test
    public void testChopLogToOrc() throws Exception {
        Path dir = Files.createTempDirectory(".");
        DataUpload upload = new DataUpload();
        int parts = upload.run("-f", "../data/sample_logs/blockTracelog", "-o",
                "-p", "%{BLOCKTRACE}", "-l", "100", "-d", dir.toString());
        File d = dir.toFile();
        FileUtils.deleteDirectory(d);
        Assert.assertEquals(2, parts);
    }
}
