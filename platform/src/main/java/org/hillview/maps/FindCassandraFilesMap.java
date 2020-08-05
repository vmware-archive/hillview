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

package org.hillview.maps;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.storage.CassandraConnectionInfo;
import org.hillview.storage.CassandraDatabase;
import org.hillview.storage.CassandraFileReference;
import org.hillview.storage.IFileReference;
import org.hillview.storage.CassandraDatabase.CassandraTokenRange;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class FindCassandraFilesMap implements IMap<Empty, List<IFileReference>> {
    static final long serialVersionUID = 1;
    private final CassandraConnectionInfo conn;

    public FindCassandraFilesMap(CassandraConnectionInfo conn) {
        this.conn = conn;
    }

    @Override
    public List<IFileReference> apply(@Nullable Empty empty) {
        List<IFileReference> result = new ArrayList<IFileReference>();
        CassandraDatabase db = new CassandraDatabase(this.conn);
        List<String> ssTables = db.getSSTablePath();
        List<CassandraTokenRange> tokenRanges = db.getTokenRanges();
        String localEndpoint = db.getLocalEndpoint();
        db.closeClusterConnection();
        for (String ssTable : ssTables)
            result.add(new CassandraFileReference(ssTable, tokenRanges, localEndpoint));
        return result;
    }
}