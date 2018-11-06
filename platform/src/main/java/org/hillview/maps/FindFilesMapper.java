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
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scans a folder and finds files matching a pattern.
 * Creates a list of file loaders that can be invoked to load the actual file data as tables.
 */
public class FindFilesMapper implements IMap<Empty, List<IFileReference>> {
    private final FileSetDescription description;

    public FindFilesMapper(FileSetDescription description) {
        this.description = description;
    }

    /**
     * Returns a list of IFileReference objects, one for each of the files
     * that match the specification.
     * @param empty: unused.
     */
    @Override
    public List<IFileReference> apply(Empty empty) {
        String folders = this.description.folder;
        // Paths for multiple directories
        Path paths[];
        String dirs[];
        if (!folders.contains(",")) {
            paths = new Path[1];
            dirs = new String[1];
            dirs[0] = this.description.folder;
            paths[0] = Paths.get(this.description.folder);
        } else {
            dirs = folders.split(",");
            paths = new Path[dirs.length];
            for (int i = 0; i < dirs.length; i++) {
                paths[i] = Paths.get(dirs[i]);
            }
        }
        @Nullable
        String filenameRegex = this.description.getRegexPattern();
        for (int i = 0; i < paths.length; i++) {
            HillviewLogger.instance.info("Find files", "folder: {0}, absfolder: {1}, regex: {2}", dirs[i],
            paths[i].toAbsolutePath().toString(), filenameRegex);
        }

        Stream<Path> files = null;
        Stream<Path> result = null;
        try {
            for (int i = 0; i < paths.length; i++) {
                if (Files.exists(paths[i])) {
                    files = Files.walk(paths[i], 1, FileVisitOption.FOLLOW_LINKS);
                    if (result == null)
                        result = files;
                    else
                        result = Stream.concat(result, files);
                 }
            }
            if (result == null) {
                return Collections.emptyList();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        result = result.filter(f -> {
            if (f == null)
                return false;
            if (filenameRegex == null)
                return true;
            Path path = f.getFileName();
            if (path == null)
                return false;
            return path.toString().matches(filenameRegex);
        });
        Stream<String> fileNames = result.map(Path::toString).sorted();
        if (this.description.repeat > 1)
            fileNames = fileNames.flatMap(
                    n -> Collections.nCopies(this.description.repeat, n).stream());
        /*
        // Sometimes useful for testing.
        if (this.description.maxCount > 0)
            fileNames = fileNames.limit(this.maxCount);
        */
        List<String> list = fileNames.collect(Collectors.toList());
        String allNames = String.join(",", list);
        HillviewLogger.instance.info("Files found", "{0}: {1}", list.size(), allNames);
        List<IFileReference> finalResult = new ArrayList<IFileReference>();
        for (String n: list)
            finalResult.add(this.description.createFileReference(n));
        return finalResult;
    }
}
