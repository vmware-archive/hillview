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

import {TopMenu, SubMenu} from "./ui/menu";
import {InitialObject} from "./initialObject";
import {RemoteObject, OnCompleteRenderer} from "./rpc";
import {ICancellable} from "./util";
import {IDataView} from "./ui/dataview";
import {ConsoleDisplay} from "./ui/errReporter";
import {FullPage} from "./ui/fullPage";

export class ControlMenu extends RemoteObject implements IDataView {
    private top: HTMLElement;
    private menu: TopMenu;
    private console: ConsoleDisplay;

    constructor(protected init: InitialObject, protected page: FullPage) {
        super(init.remoteObjectId);

        this.top = document.createElement("div");
        this.menu = new TopMenu([{
            text: "Manage", subMenu: new SubMenu([
                { text: "List machines", action: () => { this.ping(); } },
                { text: "Toggle memoization", action: () => { this.command("toggleMemoization"); } },
                { text: "Memory use", action: () => { this.command("memoryUse"); } },
                { text: "Purge memoized", action: () => { this.command("purgeMemoization"); } },
                { text: "Purge root datasets", action: () => { this.command("purgeDatasets"); } },
                { text: "Purge leaf datasets", action: () => { this.command("purgeLeftDatasets"); } }
            ])}
        ]);

        this.console = new ConsoleDisplay();
        this.page.setMenu(this.menu);
        this.top.appendChild(this.console.getHTMLRepresentation());
    }

    getHTMLRepresentation(): HTMLElement {
        return this.top;
    }

    ping(): void {
        let rr = this.createRpcRequest("ping", null);
        rr.invoke(new PingReceiver(this.page, rr));
    }

    command(command: string): void {
        let rr = this.createRpcRequest(command, null);
        rr.invoke(new CommandReceiver(command, this.page, rr));
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }

    public refresh(): void {}
}

export function insertControlMenu(): void {
    let page = new FullPage("Control center", null);
    page.append();
    let menu = new ControlMenu(InitialObject.instance, page);
    page.setDataView(menu);
}

/**
 * Corresponds to the Java class ControlMessage.Status.
 */
interface Status {
    hostname: string;
    result: string;
    exception: string;
}

/**
 * Receives the results of a remote command.
 * @param T  each individual result has this type.
 */
class CommandReceiver extends OnCompleteRenderer<Status[]> {
    public constructor(name: string, page: FullPage, operation: ICancellable) {
        super(page, operation, name);
    }

    toString(s: Status): string {
        let str = s.hostname + "=>";
        if (s.exception == null)
            str += s.result;
        else
            str += s.exception;
        return str;
    }

    run(value: Status[]): void {
        let res = "";
        for (let s of value) {
            if (res != "")
                res += "\n";
            res += this.toString(s);
        }
        this.page.reportError(res);
    }
}

class PingReceiver extends OnCompleteRenderer<string[]> {
    public constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "ping");
    }

    run(value: string[]): void {
        this.page.reportError(this.value.toString());
    }
}