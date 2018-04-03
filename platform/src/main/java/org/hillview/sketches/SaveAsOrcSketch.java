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

package org.hillview.sketches;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.ISketch;
import org.hillview.storage.OrcFileWriter;
import org.hillview.table.Schema;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ITable;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * This sketch saves a table into a set of Orc files in the specified folder.
 * TODO: Today the save can succeed on some machines, and fail on others.
 * There is no cleanup if that happens.
 * This does not return anything really.
 * If the saving fails this will trigger an exception.
 */
public class SaveAsOrcSketch implements ISketch<ITable, Empty> {
    private final String folder;
    @Nullable
    private final Schema schema;
    /**
     * If true a schema file will also be created.
     */
    private final boolean createSchema;

    public SaveAsOrcSketch(final String folder,
                           @Nullable final Schema schema, boolean createSchema) {
        this.folder = folder;
        this.createSchema = createSchema;
        this.schema = schema;
    }

    @Override
    public Empty create(ITable data) {
        try {
            if (this.schema != null)
                data = data.project(this.schema);

            List<ColumnAndConverterDescription> ccds = data.getSchema()
                    .getColumnAndConverterDescriptions();
            data.getLoadedColumns(ccds);
            File file = new File(this.folder);
            boolean ignored = file.mkdir();
            // There is a race here: multiple workers may try to create the
            // folder at the same time, so we don't bother if the creation fails.
            // If the folder can't be created the writing below will fail.

            String tableFile = data.getSourceFile();
            if (tableFile == null)
                throw new RuntimeException("I don't know how to generate file names for the data");
            String baseName = Utilities.getBasename(tableFile);
            String path = Paths.get(this.folder, baseName + ".orc").toString();
            OrcFileWriter writer = new OrcFileWriter(path);
            writer.writeTable(data);

            if (this.createSchema) {
                String schemaFile = baseName + ".schema";
                Path schemaPath = Paths.get(this.folder, schemaFile);
                data.getSchema().writeToJsonFile(schemaPath);
                Path finalSchemaPath = Paths.get(this.folder, Schema.schemaFileName);
                // Attempt to atomically rename the schema; this is also a race which
                // may be won by multiple participants.  Hopefully all the schemas
                // written should be identical, so it does not matter if this happens
                // many times.
                Files.move(schemaPath, finalSchemaPath, StandardCopyOption.ATOMIC_MOVE);
            }
            return Empty.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    @Override
    public Empty zero() {
        return Empty.getInstance();
    }

    @Nullable
    @Override
    public Empty add(@Nullable Empty left, @Nullable Empty right) {
        return left;
    }
}
