"use strict";
/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * A SchemaClass is a class containing a Schema and some indexes and methods
 * for fast access.
 */
var SchemaClass = /** @class */ (function () {
    function SchemaClass(schema) {
        this.schema = schema;
        this.columnNames = [];
        this.columnMap = new Map();
        for (var i = 0; i < this.schema.length; i++) {
            var colName = this.schema[i].name;
            this.columnNames.push(colName);
            this.columnMap.set(colName, i);
        }
    }
    SchemaClass.prototype.uniqueColumnName = function (prefix) {
        var name = prefix;
        var i = 0;
        while (this.columnMap.has(name)) {
            name = prefix + ("_" + i);
            i++;
        }
        return name;
    };
    SchemaClass.prototype.columnIndex = function (colName) {
        if (!this.columnMap.has(colName))
            return -1;
        return this.columnMap.get(colName);
    };
    SchemaClass.prototype.find = function (colName) {
        if (colName == null)
            return null;
        var colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    };
    SchemaClass.prototype.filter = function (predicate) {
        var cols = this.schema.filter(predicate);
        return new SchemaClass(cols);
    };
    SchemaClass.prototype.append = function (cd) {
        return new SchemaClass(this.schema.concat(cd));
    };
    SchemaClass.prototype.concat = function (cds) {
        return new SchemaClass(this.schema.concat(cds));
    };
    Object.defineProperty(SchemaClass.prototype, "length", {
        get: function () {
            return this.schema.length;
        },
        enumerable: true,
        configurable: true
    });
    SchemaClass.prototype.get = function (index) {
        return this.schema[index];
    };
    return SchemaClass;
}());
exports.SchemaClass = SchemaClass;
