"use strict";
/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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
var __extends = (this && this.__extends) || (function () {
    var extendStatics = Object.setPrototypeOf ||
        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
        function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
var menu_1 = require("./ui/menu");
var initialObject_1 = require("./initialObject");
var rpc_1 = require("./rpc");
var errReporter_1 = require("./ui/errReporter");
var fullPage_1 = require("./ui/fullPage");
var dialog_1 = require("./ui/dialog");
/**
 * The load menu is the first menu that is displayed on the screen.
 */
var LoadMenu = /** @class */ (function (_super) {
    __extends(LoadMenu, _super);
    function LoadMenu(init, page, showManagement) {
        var _this = _super.call(this, init.remoteObjectId) || this;
        _this.init = init;
        _this.page = page;
        _this.top = document.createElement("div");
        var items = [
            { text: "Test datasets", subMenu: new menu_1.SubMenu([
                    { text: "Flights (all)", action: function () { return init.testDataset(0, _this.page); } },
                    { text: "Flights (subset)", action: function () { return init.testDataset(1, _this.page); } },
                    { text: "vrOps", action: function () { return init.testDataset(2, _this.page); } },
                    { text: "MNIST", action: function () { return init.testDataset(3, _this.page); } },
                    { text: "Image segmentation", action: function () { return init.testDataset(4, _this.page); } },
                    { text: "Criteo (subset)", action: function () { return init.testDataset(5, _this.page); } },
                    { text: "Flights (x5)", action: function () { return init.testDataset(6, _this.page); } },
                    { text: "Flights (x10)", action: function () { return init.testDataset(7, _this.page); } },
                    { text: "cabs", action: function () { return init.testDataset(8, _this.page); } },
                ]) }, {
                text: "Load", subMenu: new menu_1.SubMenu([
                    { text: "System logs", action: function () { return init.loadLogs(_this.page); } },
                    { text: "CSV files", action: function () { return _this.showCSVFileDialog(); } },
                    { text: "DB tables", action: function () { return _this.showDBDialog(); } }
                ])
            }
        ];
        if (showManagement) {
            /**
             * These are operations supported by the back-end management API.
             * They are mostly for testing, debugging, maintenance and measurement.
             */
            items.push({ text: "Manage", subMenu: new menu_1.SubMenu([
                    { text: "List machines", action: function () { return _this.ping(); } },
                    { text: "Toggle memoization", action: function () { return _this.command("toggleMemoization"); } },
                    { text: "Memory use", action: function () { return _this.command("memoryUse"); } },
                    { text: "Purge memoized", action: function () { return _this.command("purgeMemoization"); } },
                    { text: "Purge root datasets", action: function () { return _this.command("purgeDatasets"); } },
                    { text: "Purge leaf datasets", action: function () { return _this.command("purgeLeafDatasets"); } }
                ]) });
        }
        _this.menu = new menu_1.TopMenu(items);
        _this.console = new errReporter_1.ConsoleDisplay();
        _this.page.setMenu(_this.menu);
        _this.top.appendChild(_this.console.getHTMLRepresentation());
        return _this;
    }
    LoadMenu.prototype.showDBDialog = function () {
        var _this = this;
        var dialog = new DBDialog();
        dialog.setAction(function () { return _this.init.loadDBTable(dialog.getConnection(), _this.page); });
        dialog.show();
    };
    LoadMenu.prototype.showCSVFileDialog = function () {
        var _this = this;
        var dialog = new CSVFileDialog();
        dialog.setAction(function () { return _this.init.loadCSVFiles(dialog.getFiles(), _this.page); });
        dialog.show();
    };
    LoadMenu.prototype.getHTMLRepresentation = function () {
        return this.top;
    };
    LoadMenu.prototype.ping = function () {
        var rr = this.createRpcRequest("ping", null);
        rr.invoke(new PingReceiver(this.page, rr));
    };
    LoadMenu.prototype.command = function (command) {
        var rr = this.createRpcRequest(command, null);
        rr.invoke(new CommandReceiver(command, this.page, rr));
    };
    LoadMenu.prototype.setPage = function (page) {
        if (page == null)
            throw ("null FullPage");
        this.page = page;
    };
    LoadMenu.prototype.getPage = function () {
        if (this.page == null)
            throw ("Page not set");
        return this.page;
    };
    LoadMenu.prototype.refresh = function () { };
    return LoadMenu;
}(rpc_1.RemoteObject));
exports.LoadMenu = LoadMenu;
/**
 * Dialog that asks the user which CSV files to load.
 */
var CSVFileDialog = /** @class */ (function (_super) {
    __extends(CSVFileDialog, _super);
    function CSVFileDialog() {
        var _this = _super.call(this, "Load CSV files", "Loads comma-separated value (CSV) files from all machines that are part of the service.") || this;
        _this.addTextField("folder", "Folder", dialog_1.FieldKind.String, "/", "Folder on the remote machines where all the CSV files are found.");
        _this.addTextField("fileNamePattern", "File name pattern", dialog_1.FieldKind.String, "*.csv", "Shell pattern that describes the names of the files to load.");
        _this.addTextField("schemaFile", "Schema file (optional)", dialog_1.FieldKind.String, "data.schema", "The name of a JSON file that contains the schema of the data (leave empty if no schema file exists).");
        _this.addBooleanField("hasHeader", "Header row", false, "True if first row in each file is a header row");
        return _this;
    }
    CSVFileDialog.prototype.getFiles = function () {
        return {
            schemaFile: this.getFieldValue("schemaFile"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            hasHeaderRow: this.getBooleanValue("hasHeader"),
            folder: this.getFieldValue("folder")
        };
    };
    return CSVFileDialog;
}(dialog_1.Dialog));
/**
 * Dialog asking the user which DB table to load.
 */
var DBDialog = /** @class */ (function (_super) {
    __extends(DBDialog, _super);
    function DBDialog() {
        var _this = _super.call(this, "Load DB tables", "Loads one table on each machine that is part of the service.") || this;
        // TODO: this should be a pattern string, based on local worker name.
        _this.addSelectField("databaseKind", "Database kind", ["mysql"], "mysql", "The kind of database.");
        _this.addTextField("host", "Host", dialog_1.FieldKind.String, "localhost", "Machine name where database is located; each machine will open a connection to this host");
        _this.addTextField("port", "Port", dialog_1.FieldKind.Integer, "3306", "Network port to connect to database; 3306 is the port for MySQL.");
        _this.addTextField("database", "Database", dialog_1.FieldKind.String, null, "Name of database to load.");
        _this.addTextField("table", "Table", dialog_1.FieldKind.String, null, "The name of the table to load.");
        _this.addTextField("user", "User", dialog_1.FieldKind.String, null, "The name of the user opening the connection.");
        _this.addTextField("password", "Password", dialog_1.FieldKind.Password, null, "The password for the user opening the connection.");
        return _this;
    }
    DBDialog.prototype.getConnection = function () {
        return {
            host: this.getFieldValue("host"),
            port: this.getFieldValueAsNumber("port"),
            database: this.getFieldValue("database"),
            table: this.getFieldValue("table"),
            user: this.getFieldValue("user"),
            password: this.getFieldValue("password"),
            databaseKind: this.getFieldValue("databaseKind")
        };
    };
    return DBDialog;
}(dialog_1.Dialog));
/**
 * This is the main function exposed to the web page, which causes everything
 * to get going.  It creates and displays the menu for loading data.
 */
function createLoadMenu(showManagement) {
    var page = new fullPage_1.FullPage("Load", "Load", null);
    page.append();
    var menu = new LoadMenu(initialObject_1.InitialObject.instance, page, showManagement);
    page.setDataView(menu);
}
exports.createLoadMenu = createLoadMenu;
/**
 * Receives the results of a remote management command.
 * @param T  each individual result has this type.
 */
var CommandReceiver = /** @class */ (function (_super) {
    __extends(CommandReceiver, _super);
    function CommandReceiver(name, page, operation) {
        return _super.call(this, page, operation, name) || this;
    }
    CommandReceiver.prototype.toString = function (s) {
        var str = s.hostname + "=>";
        if (s.exception == null)
            str += s.result;
        else
            str += s.exception;
        return str;
    };
    CommandReceiver.prototype.run = function (value) {
        var res = "";
        for (var _i = 0, value_1 = value; _i < value_1.length; _i++) {
            var s = value_1[_i];
            if (res != "")
                res += "\n";
            res += this.toString(s);
        }
        this.page.reportError(res);
    };
    return CommandReceiver;
}(rpc_1.OnCompleteRenderer));
/**
 * Receives and displays the result of the ping command.
 */
var PingReceiver = /** @class */ (function (_super) {
    __extends(PingReceiver, _super);
    function PingReceiver(page, operation) {
        return _super.call(this, page, operation, "ping") || this;
    }
    PingReceiver.prototype.run = function (value) {
        this.page.reportError(this.value.toString());
    };
    return PingReceiver;
}(rpc_1.OnCompleteRenderer));
//# sourceMappingURL=loadMenu.js.map