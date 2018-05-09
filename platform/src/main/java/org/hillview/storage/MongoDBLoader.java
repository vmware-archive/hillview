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

package org.hillview.storage;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;

import com.mongodb.client.MongoDatabase;
import org.hillview.utils.Converters;
import org.hillview.utils.Linq;

/**
 * Loads data from a MongoDB database.
 * The data is represented as a set of BSON documents.
 * The same assumptions on the document structure hold as for the
 * JsonFileLoader class.
 */
public class MongoDBLoader extends JsonFileLoader {
    /**
     * We don't really use JDBC, but this data structure is handy.
     */
    private final JdbcConnectionInformation info;
    private int currentRow;
    private static final JsonParser parser = new JsonParser();
    private final MongoDatabase database;
    //final DB oldDatabase;

    public MongoDBLoader(JdbcConnectionInformation info) {
        super(Converters.checkNull(info.table), null);
        this.info = info;
        assert info.database != null;
        assert info.password != null;

        MongoCredential credential = MongoCredential.createCredential(
                info.user, info.database, info.password.toCharArray());
        ServerAddress address = new ServerAddress(info.host, info.port);
        MongoClientOptions options = MongoClientOptions.builder().build();
        MongoClient client = new MongoClient(address); //, credential, options);

        this.database = client.getDatabase(info.database);
        //this.oldDatabase = client.getDB(info.database);
    }

    /**
     * Converts a MongoDB document into a GSON JsonElement.
     * @param doc  Document to convert.
     */
    private static JsonElement convert(Document doc) {
        String s = doc.toJson();
        JsonElement el = parser.parse(s);
        el.getAsJsonObject().remove("_id");
        return el;
    }

    public ITable load(int offset, int count) {
        MongoCollection<Document> collection = this.database.getCollection(info.table);
        Iterable<Document> cursor = collection.find().skip(offset).limit(count);

        // This is not the most efficient way to do things, but it is simple
        Iterable<JsonElement> data = Linq.map(cursor, MongoDBLoader::convert);

        Schema schema = this.guessSchema(filename, data.iterator());
        IAppendableColumn[] columns = schema.createAppendableColumns();

        this.currentRow = 0;
        // reopen the iterator
        for (JsonElement e: Linq.map(cursor, MongoDBLoader::convert))
            this.append(columns, e);
        ITable table = new Table(columns, this.info.table, null);
        this.close(null);
        return table;
    }
}
