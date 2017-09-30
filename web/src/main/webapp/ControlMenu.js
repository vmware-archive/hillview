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
"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var ui_1 = require("./ui");
var menu_1 = require("./menu");
var initialObject_1 = require("./initialObject");
var rpc_1 = require("./rpc");
var ControlMenu = (function (_super) {
    __extends(ControlMenu, _super);
    function ControlMenu(init, page) {
        var _this = this;
        _super.call(this, init.remoteObjectId);
        this.init = init;
        this.page = page;
        this.top = document.createElement("div");
        this.menu = new menu_1.TopMenu([{
                text: "Manage", subMenu: new menu_1.TopSubMenu([
                    { text: "List machines", action: function () { _this.ping(); } },
                    { text: "Toggle memoization", action: function () { _this.command("toggleMemoization"); } },
                    { text: "Memory use", action: function () { _this.command("memoryUse"); } },
                    { text: "Purge memoized", action: function () { _this.command("purgeMemoization"); } },
                    { text: "Purge root datasets", action: function () { _this.command("purgeDatasets"); } },
                    { text: "Purge leaf datasets", action: function () { _this.command("purgeLeftDatasets"); } }
                ]) }
        ]);
        this.console = new ui_1.ConsoleDisplay();
        this.top.appendChild(this.menu.getHTMLRepresentation());
        this.top.appendChild(this.console.getHTMLRepresentation());
    }
    ControlMenu.prototype.getHTMLRepresentation = function () {
        return this.top;
    };
    ControlMenu.prototype.ping = function () {
        var rr = this.createRpcRequest("ping", null);
        rr.invoke(new PingReceiver(this.page, rr));
    };
    ControlMenu.prototype.command = function (command) {
        var rr = this.createRpcRequest(command, null);
        rr.invoke(new CommandReceiver(command, this.page, rr));
    };
    ControlMenu.prototype.setPage = function (page) {
        if (page == null)
            throw ("null FullPage");
        this.page = page;
    };
    ControlMenu.prototype.getPage = function () {
        if (this.page == null)
            throw ("Page not set");
        return this.page;
    };
    ControlMenu.prototype.refresh = function () { };
    return ControlMenu;
}(rpc_1.RemoteObject));
exports.ControlMenu = ControlMenu;
function insertControlMenu() {
    var page = new ui_1.FullPage();
    page.append();
    var menu = new ControlMenu(initialObject_1.InitialObject.instance, page);
    page.setDataView(menu);
}
exports.insertControlMenu = insertControlMenu;
/**
 * Receives the results of a remote command.
 * @param T  each individual result has this type.
 */
var CommandReceiver = (function (_super) {
    __extends(CommandReceiver, _super);
    function CommandReceiver(name, page, operation) {
        _super.call(this, page, operation, name);
    }
    CommandReceiver.prototype.onNext = function (value) {
        _super.prototype.onNext.call(this, value);
        if (value.data != null)
            this.value = value.data;
    };
    CommandReceiver.prototype.toString = function (s) {
        var str = s.hostname + "=>";
        if (s.exception == null)
            str += s.result;
        else
            str += s.exception;
        return str;
    };
    CommandReceiver.prototype.onCompleted = function () {
        _super.prototype.finished.call(this);
        if (this.value == null)
            return;
        var res = "";
        for (var _i = 0, _a = this.value; _i < _a.length; _i++) {
            var s = _a[_i];
            if (res != "")
                res += "\n";
            res += this.toString(s);
        }
        this.page.reportError(res);
    };
    return CommandReceiver;
}(rpc_1.Renderer));
var PingReceiver = (function (_super) {
    __extends(PingReceiver, _super);
    function PingReceiver(page, operation) {
        _super.call(this, page, operation, "ping");
    }
    PingReceiver.prototype.onNext = function (value) {
        _super.prototype.onNext.call(this, value);
        if (value.data != null)
            this.value = value.data;
    };
    PingReceiver.prototype.onCompleted = function () {
        _super.prototype.finished.call(this);
        if (this.value == null)
            return;
        this.page.reportError(this.value.toString());
    };
    return PingReceiver;
}(rpc_1.Renderer));
//# sourceMappingURL=ControlMenu.js.map