"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var dialog_1 = require("./dialog");
var tableData_1 = require("./tableData");
var table_1 = require("./table");
var rpc_1 = require("./rpc");
var ColumnConverter = (function () {
    function ColumnConverter(columnName, newKind, newColumnName, table) {
        this.columnName = columnName;
        this.newKind = newKind;
        this.newColumnName = newColumnName;
        this.table = table;
    }
    ColumnConverter.dialog = function (columnName, allColumns, table) {
        var dialog = new dialog_1.Dialog("Convert column");
        dialog.addSelectField("columnName", "Column: ", allColumns, columnName);
        dialog.addSelectField("newKind", "Convert to: ", ["Category", "Json", "String", "Integer", "Double", "Date", "Interval"]);
        dialog.addTextField("newColumnName", "New column name: ", "String", columnName + " (Cat.)");
        dialog.setAction(function () {
            var kind = tableData_1.asContentsKind(dialog.getFieldValue("newKind"));
            var converter = new ColumnConverter(dialog.getFieldValue("columnName"), kind, dialog.getFieldValue("newColumnName"), table);
            converter.run();
        });
        dialog.show();
    };
    ColumnConverter.prototype.run = function () {
        var _this = this;
        if (table_1.TableView.allColumnNames(this.table.schema).indexOf(this.newColumnName) >= 0) {
            this.table.reportError("Column name " + this.newColumnName + " already exists in table.");
            return;
        }
        if (this.newKind == "Category") {
            var rr = this.table.createRpcRequest("hLogLog", this.columnName);
            rr.invoke(new HLogLogReceiver(this.table.getPage(), rr, "HLogLog", function (count) { return _this.checkValidForCategory(count); }));
        }
        else {
            this.table.reportError("Converting to " + this.newKind + " is not supported.");
        }
    };
    ColumnConverter.prototype.checkValidForCategory = function (hLogLog) {
        if (hLogLog.distinctItemCount > ColumnConverter.maxCategoricalCount) {
            this.table.reportError("Too many values for categorical column. There are " + hLogLog.distinctItemCount + ", and up to " + ColumnConverter.maxCategoricalCount + " are supported.");
        }
        else {
            this.runConversion();
        }
    };
    ColumnConverter.prototype.runConversion = function () {
        var args = {
            colName: this.columnName,
            newColName: this.newColumnName,
            newKind: this.newKind
        };
        var rr = this.table.createRpcRequest("convertColumnMap", args);
        rr.invoke(new table_1.RemoteTableReceiver(this.table.getPage(), rr));
    };
    ColumnConverter.maxCategoricalCount = 1e4;
    return ColumnConverter;
}());
exports.ColumnConverter = ColumnConverter;
var HLogLogReceiver = (function (_super) {
    __extends(HLogLogReceiver, _super);
    function HLogLogReceiver(page, operation, name, next) {
        _super.call(this, page, operation, name);
        this.next = next;
    }
    HLogLogReceiver.prototype.onNext = function (value) {
        _super.prototype.onNext.call(this, value);
        this.data = value.data;
    };
    HLogLogReceiver.prototype.onCompleted = function () {
        _super.prototype.onCompleted.call(this);
        this.next(this.data);
    };
    return HLogLogReceiver;
}(rpc_1.Renderer));
//# sourceMappingURL=columnConverter.js.map