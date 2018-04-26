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
import org.hillview.storage.FileLoaderDescription;
import org.hillview.storage.IFileLoader;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scans a folder and finds files matching a pattern.
 * Creates a list of file loaders that can be invoked to load the actual file data as tables.
 */
public class FindFilesMapper implements IMap<Empty, List<IFileLoader>> {
    private final String folder;
    private final int maxCount;
    @Nullable
    private final String fileNamePattern;
    private final FileLoaderDescription loaderDescription;
    /**
     * The cookie can be used to prevent memoization, by using a different value
     * every time.
     */
    @SuppressWarnings("FieldCanBeLocal")
    @Nullable
    private final String cookie;

    /**
     * Create an object to find all file names that match the specification.
     * @param folder   Folder where files are sought.
     * @param maxCount Maximum number of files to find.  If 0 there is no limit.
     * @param fileNamePattern  Regex for file names to search.  If null all file names match.
     * @param cookie   This string is not used except to disable memoization.
     */
    public FindFilesMapper(String folder, int maxCount,
                           @Nullable String fileNamePattern,
                           FileLoaderDescription loader,
                           @Nullable String cookie) {
        this.folder = folder;
        this.maxCount = maxCount;
        this.fileNamePattern = fileNamePattern;
        this.loaderDescription = loader;
        this.cookie = cookie;
    }

    /**
     * Create an object to find all file names that match the specification.
     * @param folder   Folder where files are sought.
     * @param maxCount Maximum number of files to find.  If 0 there is no limit.
     * @param fileNamePattern  Regex for file names to search.  If null all file names match.
     */
    public FindFilesMapper(String folder, int maxCount,
                           @Nullable String fileNamePattern,
                           FileLoaderDescription loader) {
        this(folder, maxCount, fileNamePattern, loader, null);
    }

    @Override
    public List<IFileLoader> apply(Empty empty) {
        Path dir = Paths.get(this.folder);
        HillviewLogger.instance.info("Find files", "folder: {0}, pattern: {1}",
                dir.toAbsolutePath().toString(), this.fileNamePattern);

        Stream<Path> files;
        try {
            files = Files.walk(dir, 1, FileVisitOption.FOLLOW_LINKS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        files = files.filter(f -> {
            if (this.fileNamePattern == null)
                return true;
            String filename = f.getFileName().toString();
            return filename.matches(this.fileNamePattern);
        });
        Stream<String> fileNames = files.map(Path::toString).sorted();
        if (this.maxCount > 0)
            fileNames = fileNames.limit(this.maxCount);
        List<String> list = fileNames.collect(Collectors.toList());
        String allNames = String.join(",", list);
        HillviewLogger.instance.info("Files found", "{0}: {1}", list.size(), allNames);
        List<IFileLoader> result = new ArrayList<IFileLoader>();
        for (String n: list)
            result.add(this.loaderDescription.createLoader(n));
        return result;
    }
}
