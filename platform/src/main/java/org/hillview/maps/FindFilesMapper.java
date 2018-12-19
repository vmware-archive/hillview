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
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        String[] paths = this.description.fileNamePattern.trim().split("\\s*,\\s*");
        HillviewLogger.instance.info("Find files", "pattern: {0}", this.description.fileNamePattern);
        List<File> files = new ArrayList<File>();
        FilenameFilter filter;
        for (int i = 0; i < paths.length; i++) {
            Path dir_path = Paths.get(Utilities.getFolder(paths[i]));
            if (Utilities.getWildcard(paths[i]) == null)
                filter = (d, name) -> true;
            else
                filter = new WildcardFileFilter(Utilities.getWildcard(paths[i]));

            if (Files.exists(dir_path) && Files.isDirectory(dir_path))
                files.addAll(Arrays.asList(new File(Utilities.getFolder(paths[i])).listFiles(filter)));
            else
                files.addAll(Arrays.asList(new File[0]));
        }
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
        for (String n : names)
            result.add(this.description.createFileReference(n));
        return result;
    }
}

