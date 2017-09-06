/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var rpc_1 = require("./rpc");
var tableData_1 = require("./tableData");
/// This is an abstraction for the toplevel dataset, which represents
// the full table that was loaded initially.
var TableDataSet = (function (_super) {
    __extends(TableDataSet, _super);
    // TODO: handle errors that can occur while retrieving DistinctStrings
    function TableDataSet(remoteObjectId) {
        _super.call(this, remoteObjectId);
        this.columnValues = new Map();
    }
    /// Retrieves the category values for all specified column names
    /// and stores them internally in columnValues.
    /// Invokes continuation when all values are known.
    TableDataSet.prototype.retrieveCategoryValues = function (columnNames, page, 
        // The operation is the asynchronous operation
        // that may have retrieved the data
        continuation) {
        var rr = this.createRpcRequest("uniqueStrings", columnNames);
        var renderer = new ReceiveCategory(this, continuation, page, rr);
        for (var _i = 0, columnNames_1 = columnNames; _i < columnNames_1.length; _i++) {
            var c = columnNames_1[_i];
            if (!this.columnValues.has(c))
                renderer.addColumn(c);
        }
        if (!renderer.empty())
            rr.invoke(renderer);
        else
            continuation(rr);
    };
    TableDataSet.prototype.setDistinctStrings = function (columnName, values) {
        this.columnValues.set(columnName, values);
    };
    TableDataSet.prototype.getDistinctStrings = function (columnName) {
        return this.columnValues.get(columnName);
    };
    return TableDataSet;
}(rpc_1.RemoteObject));
exports.TableDataSet = TableDataSet;
// Receives a list of DistinctStrings and stores them into a TableDataSet.
var ReceiveCategory = (function (_super) {
    __extends(ReceiveCategory, _super);
    function ReceiveCategory(tds, continuation, page, operation) {
        _super.call(this, page, operation, "Create converter");
        this.tds = tds;
        this.continuation = continuation;
        this.columns = [];
        this.values = null;
    }
    ReceiveCategory.prototype.addColumn = function (columnName) {
        this.columns.push(columnName);
    };
    ReceiveCategory.prototype.empty = function () { return this.columns.length == 0; };
    ReceiveCategory.prototype.onNext = function (value) {
        _super.prototype.onNext.call(this, value);
        this.values = value.data;
    };
    ReceiveCategory.prototype.onCompleted = function () {
        if (this.values == null)
            return;
        if (this.columns.length != this.values.length)
            throw "Asked for " + this.columns.length + " columns, got " + this.values.length;
        for (var i = 0; i < this.columns.length; i++) {
            var col = this.columns[i];
            if (this.values[i].truncated) {
                this.page.reportError("Column " + col + " has too many distinct values; it is not really a category");
            }
            else {
                var ds = new tableData_1.DistinctStrings(this.values[i]);
                this.tds.setDistinctStrings(col, ds);
            }
        }
        _super.prototype.finished.call(this);
        this.continuation(this.operation);
    };
    return ReceiveCategory;
}(rpc_1.Renderer));
//# sourceMappingURL=dataset.js.map