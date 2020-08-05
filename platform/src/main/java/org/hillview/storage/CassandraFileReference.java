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

package org.hillview.storage;

import java.io.File;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

public class CassandraFileReference implements IFileReference {
    private final String pathname;

    public CassandraFileReference(final String pathname) {
        this.pathname = pathname;
    }

    @Override
    public ITable load() {
        TextFileLoader loader;
        loader = new CassandraSSTableLoader(this.pathname, true);
        return Converters.checkNull(loader.load());
    }

    @Override
    public long getSizeInBytes() {
        File file = new File(this.pathname);
        if (file.exists())
            return file.length();
        return 0;
    }
}