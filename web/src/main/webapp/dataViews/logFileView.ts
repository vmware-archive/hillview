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

import {IHtmlElement} from "../ui/ui";
import {SubMenu, TopMenu} from "../ui/menu";
import {FindBar} from "../ui/findBar";
import {BaseRenderer, BigTableView} from "../tableTarget";
import {FullPage, PageTitle} from "../ui/fullPage";
import {convertToStringFormat, ICancellable, makeMissing, makeSpan, PartialResult} from "../util";
import {
    FindResult, GenericLogs,
    NextKList,
    RemoteObjectId, Schema, StringFilterDescription
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {IUpdate, RangeView} from "../ui/rangeView";
import {Receiver, RpcRequest} from "../rpc";

export class LogFileView extends BigTableView implements IHtmlElement, IUpdate {
    protected readonly topLevel: HTMLElement;
    protected readonly findBar: FindBar;
    protected nextKList: NextKList;
    protected visibleColumns: Set<string>;
    protected rangeView: RangeView;
    protected color: Map<string, string>;  // one per column
    public static readonly requestSize = 1000;  // number of lines brought in one request
    private activeRequest: RpcRequest<PartialResult<NextKList>>;

    constructor(remoteObjectId: RemoteObjectId,
                schema: SchemaClass,
                page: FullPage,
                protected filename: string) {
        super(remoteObjectId, 0 /* we don't know the row count yet */, schema, page, "LogFileView");
        this.visibleColumns = new Set<string>();
        this.color = new Map<string, string>();

        this.topLevel = document.createElement("div");
        this.topLevel.className = "logFileViewer";

        const menu = new TopMenu([{
            text: "View",
            help: "Control the view",
            subMenu: new SubMenu([{
                text: "Wrap",
                help: "Change text wrapping",
                action: () => this.rangeView.toggleWrap()
            }, {
                text: "Refresh",
                help: "Refresh current view",
                action: () => {
                    if (this.rowCount === 0)
                        return;
                    const [f, _, i] = this.rangeView.rowsVisible();
                    let start = f == null ? i : f;
                    start = Math.max(start - 100, 0);
                    const count = Math.min(LogFileView.requestSize, this.rowCount - start);
                    this.download(start, count, f);
                }
            }])
        }, {
            text: "Find",
            help: "Search specific values",
            subMenu: new SubMenu([{
                text: "Find...",
                help: "Search for a string",
                action: () => this.showFindBar(true)
            }])
        }]);
        this.page.setMenu(menu);

        this.findBar = new FindBar((n, f) => this.onFind(n, f));
        this.topLevel.appendChild(this.findBar.getHTMLRepresentation());
        this.findBar.show(false);

        const schemaDisplay = document.createElement("div");
        this.topLevel.appendChild(schemaDisplay);
        this.displaySchema(schemaDisplay);

        this.rangeView = new RangeView(this);
        this.rangeView.setMax(0);
        this.topLevel.appendChild(this.rangeView.getHTMLRepresentation());
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

    public download(start: number, count: number, firstRow: number): void {
        if (this.activeRequest != null) {
            this.page.cancel(this.activeRequest);
            this.activeRequest = null;
        }
        console.log("Downloading " + start + " and " + count);
        const rr = this.createGetLogFragmentRequest(
            this.schema.schema, start,
            null, null, count);
        this.activeRequest = rr;
        rr.invoke(new LogFragmentReceiver(this.page, this, rr, firstRow, null));
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

    public refresh(): void {
        this.display(this.nextKList, this.rangeView.firstRow);
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

    private onFind(filter: StringFilterDescription, fromTop: boolean): void {
        // TODO
    }

    private showFindBar(show: boolean): void {
        this.findBar.show(show);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public display(nextKList: NextKList, firstRow: number): void {
        this.nextKList = nextKList;
        if (nextKList == null)
            return;

        this.rowCount = nextKList.rowsScanned;
        this.rangeView.setMax(nextKList.rowsScanned);
        const elements = [];
        const cols = this.schema.schema;
        for (const row of nextKList.rows) {
            const rowSpan = document.createElement("span");
            rowSpan.className = "logRow";
            let index = 0;
            for (const value of row.values) {
                const col = cols[index++];
                if (this.visibleColumns.has(col.name)) {
                    if (value == null) {
                        rowSpan.appendChild(makeMissing());
                    } else {
                        let shownValue = convertToStringFormat(value, col.kind);
                        let high;
                        if (col.name === GenericLogs.lineNumberColumn) {
                            // left pad the line number
                            shownValue = ("00000" + shownValue).slice(-5);
                            high = makeSpan(shownValue, false);
                        } else {
                            high = this.findBar.highlight(shownValue);
                        }
                        high.classList.add("logCell");
                        high.style.color = this.color.get(col.name);
                        rowSpan.appendChild(high);
                    }
                }
            }
            rowSpan.appendChild(document.createElement("br"));
            elements.push(rowSpan);
        }
        if (elements.length !== 0)
            this.rangeView.downloaded(elements, nextKList.startPosition, firstRow);
    }

    public updateView(nextKList: NextKList,
                      firstRow: number,
                      result: FindResult): void {
        this.activeRequest = null;
        this.display(nextKList, firstRow);
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer {
        return null;
    }

    public resize(): void {
        this.refresh();
    }
}

export class LogFileReceiver extends BaseRenderer {
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

class PrunedLogFileReceiver extends BaseRenderer {
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
        const newPage = new FullPage(0, new PageTitle(this.filename), null, this.page.dataset);
        const drop = this.schema.filter((c) => c.name !== GenericLogs.directoryColumn &&
                                               c.name !== GenericLogs.filenameColumn);
        const viewer = new LogFileView(
            this.remoteObject.remoteObjectId, drop, newPage, this.filename);
        newPage.setSinglePage(viewer);
        logWindow.onload = () => {
            logWindow.document.title = this.filename;
            logWindow.document.body.appendChild(newPage.getHTMLRepresentation());
        };
        const rr = viewer.createGetLogFragmentRequest(
            drop.schema, this.row == null ? 0 : -1, this.row, this.rowSchema, LogFileView.requestSize);
        rr.invoke(new LogFragmentReceiver(newPage, viewer, rr, -1, null));
    }
}

class LogFragmentReceiver extends Receiver<NextKList> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<NextKList>,
                protected firstRow: number,
                protected result: FindResult) {
        super(page, operation, "Getting log fragment");
    }

    public onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.view.updateView(value.data, this.firstRow, this.result);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}
