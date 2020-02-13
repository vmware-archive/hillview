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

import {IHtmlElement, removeAllChildren} from "../ui/ui";
import {SubMenu, TopMenu} from "../ui/menu";
import {FindBar} from "../ui/findBar";
import {BaseReceiver, OnNextK, TableTargetAPI} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {Converters, ICancellable, makeMissing, makeSpan, significantDigits} from "../util";
import {FindResult, GenericLogs, NextKList, RecordOrder, RemoteObjectId, Schema} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {NextKReceiver} from "./tableView";

export class LogFileView extends TableTargetAPI implements IHtmlElement, OnNextK {
    protected readonly topLevel: HTMLElement;
    protected readonly findBar: FindBar;
    protected readonly footer: HTMLElement;
    protected readonly contents: HTMLElement;
    protected nextKList: NextKList;
    protected visibleColumns: Set<string>;
    protected color: Map<string, string>;  // one per column
    protected wrap = true;

    constructor(remoteObjectId: RemoteObjectId,
                protected schema: SchemaClass,
                protected filename: string) {
        super(remoteObjectId);
        this.visibleColumns = new Set<string>();
        this.color = new Map<string, string>();

        this.topLevel = document.createElement("div");
        this.topLevel.className = "logFileViewer";

        const header = document.createElement("header");
        this.topLevel.appendChild(header);

        const titleBar = document.createElement("div");
        titleBar.className = "logFileTitle";
        const wrap = document.createElement("div");
        header.appendChild(wrap);
        wrap.appendChild(titleBar);

        this.findBar = new FindBar((n, f) => this.onFind(n, f), null);
        header.appendChild(this.findBar.getHTMLRepresentation());
        this.findBar.show(false);

        const schemaDisplay = document.createElement("div");
        header.appendChild(schemaDisplay);
        this.displaySchema(schemaDisplay);

        this.contents = document.createElement("div");
        this.contents.className = "logFileContents";
        this.topLevel.appendChild(this.contents);

        this.footer = document.createElement("footer");
        this.footer.className = "logFileFooter";
        this.topLevel.appendChild(this.footer);

        const menu = new TopMenu([{
            text: "View",
            help: "Control the view",
            subMenu: new SubMenu([{
                text: "Wrap",
                help: "Change text wrapping",
                action: () => this.toggleWrap()
            }])
        } /* TODO, {
            text: "Find",
            help: "Search specific values",
            subMenu: new SubMenu([{
                text: "Find...",
                help: "Search for a string",
                action: () => this.showFindBar(true)
            }])
        } */]);

        // must be done after the menu has been created
        titleBar.appendChild(menu.getHTMLRepresentation());
        const h2 = document.createElement("h2");
        h2.textContent = this.filename;
        h2.style.textAlign = "center";
        h2.style.flexGrow = "100";
        titleBar.appendChild(h2);
    }

    protected toggleWrap(): void {
        this.wrap = !this.wrap;
        if (!this.wrap) {
            this.contents.style.whiteSpace = "nowrap";
        } else {
            this.contents.style.whiteSpace = "normal";
        }
    }

    private displaySchema(header: HTMLElement): void {
        const tbl = document.createElement("table");
        tbl.style.tableLayout = "fixed";
        tbl.style.width = "100%";
        header.appendChild(tbl);
        const row = document.createElement("tr");
        row.className = "logHeader";
        tbl.appendChild(row);
        for (const col of this.schema.columnNames) {
            const cell = row.insertCell();
            cell.textContent = col;
            cell.onclick = () => this.rotateColor(col, cell);
            const check = document.createElement("input");
            check.type = "checkbox";
            let checked = true;
            if (col === GenericLogs.filenameColumn ||
                col === GenericLogs.directoryColumn)
                checked = false;
            check.checked = checked;
            check.onclick = (e) => { this.check(col); e.stopPropagation(); };
            cell.appendChild(check);
            if (checked)
                this.visibleColumns.add(col);
            this.color.set(col, "black");
        }
    }

    private static nextColor(current: string): string {
        switch (current) {
            case "black":
                return "red";
            case "red":
                return "blue";
            case "blue":
                return "green";
            default:
                return "black";
        }
    }

    private refresh(): void {
        this.display(this.nextKList);
    }

    private rotateColor(col: string, cell: HTMLElement): void {
        const current = this.color.get(col);
        const next = LogFileView.nextColor(current);
        this.color.set(col, next);
        cell.style.color = this.color.get(col);
        this.refresh();
    }

    private check(col: string): void {
        if (this.visibleColumns.has(col))
            this.visibleColumns.delete(col);
        else
            this.visibleColumns.add(col);
        this.refresh();
    }

    private onFind(next: boolean, fromTop: boolean): void {
        // TODO
    }

    private showFindBar(show: boolean): void {
        this.findBar.show(show);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public updateCompleted(timeInMs: number): void {
        this.footer.textContent = "Operation took " +
            significantDigits(timeInMs / 1000) + " seconds";
    }

    public display(nextKList: NextKList): void {
        this.nextKList = nextKList;
        removeAllChildren(this.contents);
        if (nextKList == null)
            return;

        const parent = document.createElement("div");
        for (const row of nextKList.rows) {
            const rowSpan = document.createElement("span");
            let index = 0;
            for (const value of row.values) {
                const col = (this.schema.schema)[index++];
                if (this.visibleColumns.has(col.name)) {
                    if (value == null) {
                        rowSpan.appendChild(makeMissing());
                    } else {
                        let shownValue = Converters.valueToString(value, col.kind);
                        if (col.name === GenericLogs.lineNumberColumn) {
                            // left pad the line number
                            shownValue = ("00000" + shownValue).slice(-5);
                        }
                        const high = makeSpan(shownValue, false);
                        high.classList.add("logCell");
                        high.style.color = this.color.get(col.name);
                        rowSpan.appendChild(high);
                    }
                }
            }
            rowSpan.appendChild(document.createElement("br"));
            parent.appendChild(rowSpan);
        }
        this.contents.appendChild(parent);
    }

    public updateView(nextKList: NextKList,
                      ignored0: boolean,
                      ignored1: RecordOrder,
                      result: FindResult): void {
        this.display(nextKList);
    }
}

export class LogFileReceiver extends BaseReceiver {
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       protected filename: string,
                       protected schema: SchemaClass,
                       protected rowSchema: Schema,
                       protected row: any[]) {
        super(page, operation, "GetLogFile", page.dataset);
    }

    public run(): void {
        super.run();
        // Prune the dataset; may increase efficiency
        const rr = this.remoteObject.createStreamingRpcRequest<RemoteObjectId>("prune", null);
        rr.chain(this.operation);
        const observer = new PrunedLogFileReceiver(this.page, rr, this.filename,
            this.schema, this.rowSchema, this.row);
        rr.invoke(observer);
    }
}

class PrunedLogFileReceiver extends BaseReceiver {
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       protected filename: string,
                       protected schema: SchemaClass,
                       protected rowSchema: Schema,
                       protected row: any[]) {
        super(page, operation, "prune", page.dataset);
    }

    public run(): void {
        super.run();
        const logWindow = window.open("log.html", "_blank");
        const viewer = new LogFileView(this.remoteObject.remoteObjectId, this.schema, this.filename);
        logWindow.onload = () => {
            logWindow.document.title = this.filename;
            logWindow.document.body.appendChild(viewer.getHTMLRepresentation());
        };
        const rr = viewer.createGetLogFragmentRequest(this.schema.schema, this.row, this.rowSchema, 1000);
        rr.invoke(new NextKReceiver(this.page, viewer, rr, false, null, null));
    }
}
