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

package org.hillview.maps;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.utils.HillviewLogging;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scans a folder and finds files matching a pattern.
 */
public class FindFilesMapper implements IMap<Empty, List<String>> {
    private final String folder;
    private final int maxCount;
    @Nullable
    private final String fileNamePattern;

    /**
     * Create an object to find and create CsvFileObjects.
     * @param folder   Folder where files are sought.
     * @param maxCount Maximum number of files to find.  If 0 there is no limit.
     * @param fileNamePattern  Regex for file names to search.  If null all file names match.
     */
    public FindFilesMapper(String folder, int maxCount,
                           @Nullable String fileNamePattern) {
        this.folder = folder;
        this.maxCount = maxCount;
        this.fileNamePattern = fileNamePattern;
    }

    @Override
    public List<String> apply(Empty empty) {
        Path currentRelativePath = Paths.get("");
        String cwd = currentRelativePath.toAbsolutePath().toString();
        HillviewLogging.logger().info("Current directory is: " + cwd);

        Path folder = Paths.get(this.folder);
        Stream<Path> files;
        try {
            files = Files.walk(folder, 1, FileVisitOption.FOLLOW_LINKS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        files = files.filter(f -> {
            if (fileNamePattern == null)
                return true;
            String filename = f.getFileName().toString();
            return filename.matches(this.fileNamePattern);
        });
        if (this.maxCount > 0)
            files = files.limit(this.maxCount);
        return files.sorted(Comparator.comparing(Path::toString))
                .map(f -> f.getFileName().toString()).collect(Collectors.toList());
    }
}
