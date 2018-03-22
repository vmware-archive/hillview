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
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.storage.JdbcDatabase;

import java.sql.SQLException;

public class LoadDatabaseTableMapper implements IMap<Empty, ITable> {
    private final JdbcConnectionInformation conn;

    public LoadDatabaseTableMapper(JdbcConnectionInformation conn) {
        this.conn = conn;
    }

    @Override
    public ITable apply(Empty data) {
        try {
            JdbcDatabase db = new JdbcDatabase(this.conn);
            db.connect();
            ITable result = db.readTable();
            db.disconnect();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
