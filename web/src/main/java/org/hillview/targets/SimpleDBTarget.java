/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.targets;

import org.hillview.*;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;

import java.sql.SQLException;

/**
 * This targets represents a simple database that is accessed directly using SQL from
 * the front-end.
 */
public final class SimpleDBTarget extends RpcTarget {
    private final JdbcConnectionInformation jdbc;
    // TODO: no table, send direct SQL
    private final ITable table;

    SimpleDBTarget(JdbcConnectionInformation jdbc, HillviewComputation computation) {
        super(computation);
        this.jdbc = jdbc;
        this.registerObject();
        JdbcDatabase db = new JdbcDatabase(this.jdbc);
        try {
            db.connect();
            this.table = db.readTable();
            db.disconnect();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return this.jdbc.toString();
    }

    @HillviewRpc
    public void getSchema(RpcRequest request, RpcRequestContext context) {
        Schema schema = table.getSchema();
        this.returnResultDirect(request, context, schema);
    }
}
