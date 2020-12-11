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
import {BaseReceiver, BigTableView} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, Converters, ICancellable, makeSpan, PartialResult, percentString, px} from "../util";
import {
    BucketsInfo,
    FindResult,
    GenericLogs,
    Groups,
    IColumnDescription,
    NextKList,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {OnCompleteReceiver, Receiver, RpcRequest} from "../rpc";
import {TableMeta} from "../ui/receiver";
import {DataRangesReceiver} from "./dataRangesReceiver";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {AxisData} from "./axisData";
import {TimestampPlot} from "../ui/timestampPlot";

export class LogFileView extends BigTableView implements IHtmlElement {
    protected readonly topLevel: HTMLElement;
    protected readonly findBar: FindBar;
    protected nextKList: NextKList;
    protected visibleColumns: Set<string>;
    protected color: Map<string, string>;  // one per column
    public static readonly requestSize = 1000;  // number of lines brought in one request
    private activeRequest: RpcRequest<NextKList> | null;
    protected contents: HTMLDivElement;
    protected wrap: boolean = false;
    protected bars: HTMLDivElement[];
    protected plots: TimestampPlot[];
    public readonly heatmapWidth: number = 10;
    protected tsIndex: number; // index of the timestamp column in data
    private maxTs: number; // maximum timestamp
    private minTs: number; // minimum timestamp
    private box: HTMLDivElement;  // box showing the visible data as an outline

    constructor(remoteObjectId: RemoteObjectId,
                meta: TableMeta,
                protected order: RecordOrder,
                public readonly timestampColumn: IColumnDescription,
                page: FullPage) {
        super(remoteObjectId, meta, page, "LogFile");
        this.visibleColumns = new Set<string>();
        this.color = new Map<string, string>();
        this.topLevel.className = "logFileViewer";
        this.bars = [];
        this.tsIndex = this.meta.schema.columnIndex(this.timestampColumn.name);

        const header = document.createElement("header");
        header.style.flex = "none";
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

        const menu = new TopMenu([{
            text: "View",
            help: "Control the view",
            subMenu: new SubMenu([{
                text: "Wrap",
                help: "Change text wrapping",
                action: () => this.toggleWrap()
            }, {
                text: "Refresh",
                help: "Refresh current view",
                action: () => {
                    // TODO
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

        const middle = this.makeToplevelDiv("logFileContents");
        middle.className = "logFileContents";

        const div = document.createElement("div");
        div.style.flex = "1";
        div.style.minWidth = "100px";
        middle.appendChild(div);

        this.contents = document.createElement("div");
        div.appendChild(this.contents);
        this.contents.style.whiteSpace = "nowrap";
        this.contents.className = "logFileData";

        const bars = document.createElement("div");
        bars.style.display = "flex";
        bars.style.flexDirection = "row";
        bars.style.flexWrap = "nowrap";
        bars.style.position = "relative";
        middle.appendChild(bars);
        const barCount = 3;
        for (let i = 0; i < barCount; i++) {
            const bar = document.createElement("div");
            bar.className = "logHeatmap";
            bar.style.width = px(this.heatmapWidth);
            this.bars.push(bar);
            bars.appendChild(bar);
        }
        this.box = document.createElement("div");
        this.box.style.position = "absolute";
        this.box.style.left = px(0);
        this.box.style.right = px(0);
        this.box.style.border = "2px solid black";
        bars.appendChild(this.box);

        const footer = document.createElement("footer");
        footer.className = "logFileFooter";
        this.topLevel.appendChild(footer);
    }

    protected createSurface(div: HTMLDivElement): PlottingSurface {
        return new HtmlPlottingSurface(div, this.page, {
            width: this.heatmapWidth,
            height: this.getHeatmapHeight(),
            topMargin: 0,
            bottomMargin: 0,
            leftMargin: 0,
            rightMargin: 0,
        });
    }

    public createSurfaces(): void {
        this.plots = this.bars.map(b => this.createSurface(b)).map(s => new TimestampPlot(s, "blue"));
    }

    public toggleWrap(): void {
        this.wrap = !this.wrap;
        if (!this.wrap) {
            this.contents.style.whiteSpace = "nowrap";
        } else {
            this.contents.style.whiteSpace = "normal";
        }
        this.resize();
    }

    public getHeatmapHeight(): number {
        return this.contents.clientHeight;
    }

    protected export(): void {
        throw new Error("Method not implemented.");
    }

    private displaySchema(header: HTMLElement): void {
        const tbl = document.createElement("table");
        tbl.style.tableLayout = "fixed";
        tbl.style.width = "100%";
        header.appendChild(tbl);
        const row = document.createElement("tr");
        row.className = "logHeader";
        tbl.appendChild(row);
        for (const col of this.order.sortOrientationList) {
            const colName = col.columnDescription.name;
            const cell = row.insertCell();
            cell.textContent = colName;
            cell.style.textOverflow = "ellipsis";
            cell.style.overflow = "hidden";
            cell.title = colName + ": Click to color; right-click to toggle.";
            cell.onclick = () => this.rotateColor(colName, cell);
            cell.oncontextmenu = (e) => { this.check(colName, cell); e.preventDefault(); }
            cell.classList.add("selected");
            this.color.set(colName, "black");
            this.visibleColumns.add(colName);
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

    public refresh(): void {
        let firstRow = null;
        if (this.nextKList != null && this.nextKList.rows.length > 0)
            firstRow = this.nextKList.rows[0].values;
        const rr = this.createNextKRequest(this.order, firstRow, 100, null, null);
        rr.invoke(new LogFragmentReceiver(this.page, this, rr));
    }

    private rotateColor(col: string, cell: HTMLElement): void {
        const current = this.color.get(col);
        const next = LogFileView.nextColor(current);
        this.color.set(col, next);
        cell.style.color = this.color.get(col);
        this.resize();
    }

    private check(colName: string, cell: HTMLElement): void {
        if (this.visibleColumns.has(colName)) {
            this.visibleColumns.delete(colName);
            cell.classList.remove("selected");
        } else {
            this.visibleColumns.add(colName);
            cell.classList.add("selected");
        }
        this.resize();
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

    public updateView(nextKList: NextKList,
                      result: FindResult | null): void {
        this.activeRequest = null;
        this.nextKList = nextKList;
        if (nextKList == null)
            return;

        removeAllChildren(this.contents);
        for (const row of nextKList.rows) {
            const rowSpan = document.createElement("span");
            rowSpan.className = "logRow";
            let index = 0;
            for (const value of row.values) {
                const col = this.order.sortOrientationList[index++];
                const name = col.columnDescription.name;
                if (!this.visibleColumns.has(name))
                    continue;
                if (value != null) {
                    let shownValue = Converters.valueToString(value, col.columnDescription.kind, true);
                    let high;
                    if (name === GenericLogs.lineNumberColumn) {
                        // left pad the line number
                        shownValue = ("00000" + shownValue).slice(-5);
                        high = makeSpan(shownValue, false);
                    } else {
                        high = this.findBar.highlight(shownValue, null);
                    }
                    high.classList.add("logCell");
                    high.style.color = this.color.get(name);
                    rowSpan.appendChild(high);
                }
            }
            rowSpan.appendChild(document.createElement("br"));
            this.contents.appendChild(rowSpan);
        }
        if (this.nextKList.rows.length > 0) {
            const minVisTs = nextKList.rows[0].values[this.tsIndex] as number;
            const maxVisTs = nextKList.rows[nextKList.rows.length - 1].values[this.tsIndex] as number;
            const minFraction = minVisTs / (this.maxTs - this.minTs);
            const maxFraction = maxVisTs / (this.maxTs - this.minTs);
            this.box.style.top = Math.round(minFraction * 100) + "%";
            this.box.style.bottom = Math.round(maxFraction * 100) + "%";
        }
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        assert(false);
    }

    public resize(): void {
        this.updateView(this.nextKList, null);
    }

    public updateLineDensity(axis: AxisData, value: Groups<number>): void {
        this.plots[0].setHistogram(value);
        this.plots[0].draw();
    }

    setTimetstampRange(min: number, max: number): void {
        this.minTs = min;
        this.maxTs = max;
    }
}

export class LogFragmentReceiver extends Receiver<NextKList> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<NextKList>) {
        super(page, operation, "Getting log fragment");
    }

    public onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.view.updateView(value.data, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.createSurfaces();
        const rr = this.view.createDataQuantilesRequest([this.view.timestampColumn], this.page, "LogFile");
        rr.chain(this.operation);
        const rec = new TimestampRangeReceiver(this.page, this.view, rr);
        rr.invoke(rec);
    }
}

export class TimestampRangeReceiver extends OnCompleteReceiver<BucketsInfo[]> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<BucketsInfo[]>) {
        super(page, operation, "Getting timestamp range");
    }

    public run(value: BucketsInfo[]) {
        assert(value.length == 1);
        const range = value[0];
        const pixels = this.view.getHeatmapHeight() / 2;
        // noinspection JSSuspiciousNameCombination
        const args = DataRangesReceiver.computeHistogramArgs(
            this.view.timestampColumn,
            range,
            pixels,
            true,
            // This is sideways
            { height: this.view.heatmapWidth, width: pixels });
        this.view.setTimetstampRange(range.min!, range.max!);
        const rr = this.view.createHistogramRequest({
            histos: [ args ],
            samplingRate: 1.0,
            seed: 0,
        });
        rr.chain(this.operation);
        const axis = new AxisData(this.view.timestampColumn, range, pixels);
        const rec = new TimestampHistogramReceiver(this.page, this.view, axis, rr);
        rr.invoke(rec);
    }
}

export class TimestampHistogramReceiver extends Receiver<Groups<number>> {
    constructor(page: FullPage,
                protected view: LogFileView,
                protected axis: AxisData,
                operation: ICancellable<Groups<number>>) {
        super(page, operation, "Getting time distribution");
    }

    public onNext(value: PartialResult<Groups<number>>): void {
        if (value != null && value.data != null)
            this.view.updateLineDensity(this.axis, value.data);
    }

    public onCompleted() {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}