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

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
<<<<<<< HEAD
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Pattern;
=======
>>>>>>> 87ea1556d66a3e03df8d8fa1d18e180aeb8b9c39

/**
 * Scans a folder and finds files matching a pattern. Creates a list of file
 * loaders that can be invoked to load the actual file data as tables.
 */
public class FindFilesMapper implements IMap<Empty, List<IFileReference>> {
    private final FileSetDescription description;

    public FindFilesMapper(FileSetDescription description) {
        this.description = description;
    }

    /**
     * Returns a list of IFileReference objects, one for each of the files that
     * match the specification.
     * 
     * @param empty:
     *            unused.
     */
    @Override
    public List<IFileReference> apply(Empty empty) {
<<<<<<< HEAD
        String folders = this.description.folder;
        // Paths for multiple directories
        Path dirPath;
        String dirs[];

        dirs = folders.split(",");
        // paths = new Path[dirs.length];

        @Nullable
        String filenameRegex = this.description.getRegexPattern();

        HillviewLogger.instance.info("Find files", "folder: {0}, absfolder: {1}, regex: {2}", folders, filenameRegex);

        Stream<Path> files = null;
        Stream<Path> result = null;
        try {
            for (int i = 0; i < dirs.length; i++) {
                dirPath = Paths.get(dirs[i]);
                if (Files.exists(dirPath)) {
                    files = Files.walk(dirPath, 1, FileVisitOption.FOLLOW_LINKS);
                    files = files.filter(f -> {
                        if (f == null)
                            return false;
                        if (filenameRegex == null)
                            return true;
                        Path path = f.getFileName();
                        if (path == null)
                            return false;
                        return Pattern.matches(filenameRegex, path.toString());
                    });
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

        Stream<String> fileNames = result.map(Path::toString).sorted();
        if (this.description.repeat > 1)
            fileNames = fileNames.flatMap(n -> Collections.nCopies(this.description.repeat, n).stream());
        /*
         * // Sometimes useful for testing. if (this.description.maxCount > 0) fileNames
         * = fileNames.limit(this.maxCount);
         */
        List<String> list = fileNames.collect(Collectors.toList());
        String allNames = String.join(",", list);
        HillviewLogger.instance.info("Files found", "{0}: {1}", list.size(), allNames);
        List<IFileReference> finalResult = new ArrayList<IFileReference>();
        for (String n : list)
            finalResult.add(this.description.createFileReference(n));
        return finalResult;
=======
        Path dir = Paths.get(this.description.getFolder());
        HillviewLogger.instance.info("Find files", "pattern: {0}",
                this.description.fileNamePattern);

        File[] files;
        FilenameFilter filter;
        if (this.description.getWildcard() == null)
            filter = (d, name) -> true;
        else
            filter = new WildcardFileFilter(this.description.getWildcard());

        if (Files.exists(dir) && Files.isDirectory(dir))
            files = new File(this.description.getFolder()).listFiles(filter);
        else
            files = new File[0];

        List<String> names = new ArrayList<String>();
        if (files != null)
            for (File f : files)
                names.add(f.getPath());
        Collections.sort(names);
        String allNames = String.join(",", names);
        HillviewLogger.instance.info("Files found", "{0}: {1}", names.size(), allNames);

        if (this.description.repeat > 1) {
            int size = names.size();
            for (int i = 0; i < size; i++)
                for (int j = 0; j < this.description.repeat - 1; j++)
                    names.add(names.get(i));
        }

        List<IFileReference> result = new ArrayList<IFileReference>();
        for (String n: names)
            result.add(this.description.createFileReference(n));
        return result;
>>>>>>> 87ea1556d66a3e03df8d8fa1d18e180aeb8b9c39
    }
}

