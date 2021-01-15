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

import {HtmlString, IHtmlElement, removeAllChildren} from "../ui/ui";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {FindBar} from "../ui/findBar";
import {BaseReceiver, BigTableView} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {
    assert,
    Converters,
    formatNumber,
    ICancellable,
    makeSpan,
    PartialResult,
    px,
    last,
    fractionToPercent, argmin, createComparisonFilter, cloneArray,
} from "../util";
import {
    BucketsInfo, Comparison,
    FindResult,
    GenericLogs,
    Groups,
    IColumnDescription,
    NextKList,
    RecordOrder,
    RemoteObjectId, RowValue
} from "../javaBridge";
import {OnCompleteReceiver, Receiver} from "../rpc";
import {TableMeta} from "../ui/receiver";
import {DataRangesReceiver} from "./dataRangesReceiver";
import {TimestampPlot} from "../ui/timestampPlot";
import {interpolateBlues} from "d3-scale-chromatic";

class FilteredDataset {
    constructor(public readonly remoteObjectId: RemoteObjectId, 
                public readonly meta: TableMeta, 
                public readonly title: string,
                // Value of nextKRows before filter was applied
                public readonly previousNextK: NextKList) {}
}

export class LogFileView extends BigTableView implements IHtmlElement {
    protected readonly topLevel: HTMLElement;
    protected readonly findBar: FindBar;
    public nextKList: NextKList;
    protected visibleColumns: Set<string>;
    protected color: Map<string, string>;  // one per column
    public static readonly requestSize = 1000;  // number of lines brought in one request
    protected contents: HTMLDivElement;
    protected wrap: boolean = false;
    protected bars: HTMLDivElement[];
    protected plots: TimestampPlot[];
    public readonly heatmapWidth: number = 15;
    protected tsIndex: number; // index of the timestamp column in data
    private maxTs: number; // maximum timestamp
    private minTs: number; // minimum timestamp
    private readonly box: HTMLDivElement;  // box showing the visible data as an outline
    private readonly linePointer: HTMLDivElement;
    public static readonly increment = 250; // how many more rows to bring
    protected readonly barHolder: HTMLDivElement;
    protected contextMenu: ContextMenu;
    protected readonly filterHolder: HTMLDivElement;
    protected filters: FilteredDataset[];

    constructor(remoteObjectId: RemoteObjectId,
                meta: TableMeta,
                protected readonly order: RecordOrder,
                public readonly timestampColumn: IColumnDescription,
                page: FullPage) {
        super(remoteObjectId, meta, page, "LogFile");
        this.visibleColumns = new Set<string>();
        this.color = new Map<string, string>();
        this.topLevel.className = "logFileViewer";
        this.bars = [];
        this.tsIndex = 0; // always first in sort order, and thus in NextKList
        this.filters = [];

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

        this.filterHolder = document.createElement("div");
        header.appendChild(this.filterHolder);

        const schemaDisplay = document.createElement("div");
        header.appendChild(schemaDisplay);
        this.displaySchema(schemaDisplay);

        this.contextMenu = new ContextMenu(this.topLevel);
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
                action: () => this.refresh()
            }])
        }/*, {
            text: "Find",
            help: "Search specific values",
            subMenu: new SubMenu([{
                text: "Find...",
                help: "Search for a string",
                action: () => this.showFindBar(true)
            }])
        } */]);
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

        this.barHolder = document.createElement("div");
        this.barHolder.style.display = "flex";
        this.barHolder.style.flexDirection = "row";
        this.barHolder.style.flexWrap = "nowrap";
        this.barHolder.style.position = "relative";
        middle.appendChild(this.barHolder);
        const barCount = 1;
        for (let i = 0; i < barCount; i++) {
            const bar = document.createElement("div");
            bar.className = "logHeatmap";
            bar.style.width = px(this.heatmapWidth);
            this.bars.push(bar);
            this.barHolder.appendChild(bar);
        }
        this.box = document.createElement("div");
        this.box.style.position = "absolute";
        this.box.style.left = px(0);
        this.box.style.right = px(0);
        this.box.style.border = "2px solid black";
        this.linePointer = document.createElement("div");
        this.linePointer.style.position = "absolute";
        this.linePointer.style.left = px(0);
        this.linePointer.style.right = px(0);
        this.linePointer.style.border = "1px solid rgba(255, 255, 0, .5)";
        this.linePointer.style.background = "rgba(255, 255, 0, .5)";
        this.linePointer.style.height = px(0);
        this.barHolder.appendChild(this.box);
        this.barHolder.appendChild(this.linePointer);
        this.barHolder.onclick = (e) => this.scrollTimestamp(e);
        this.barHolder.onmousemove = (e) => this.onMouseMove(e);
        const footer = document.createElement("footer");
        footer.className = "logFileFooter";
        this.topLevel.appendChild(footer);
        const summary = this.createDiv("summary");
        footer.appendChild(summary);
    }
    
    public getRemoteObjectId(): RemoteObjectId | null {
        if (this.filters.length > 0)
            return last(this.filters)!.remoteObjectId;
        return super.getRemoteObjectId()!;
    }

    public createSurfaces(): void {
        this.plots = this.bars.map(
            b => new TimestampPlot(b, interpolateBlues));
    }

    public timestampPosition(ts: number): number {
        if (this.minTs >= this.maxTs)
            return 0;
        if (ts < this.minTs)
            return 0;
        if (ts > this.maxTs)
            return 1;
        return (ts - this.minTs) / (this.maxTs - this.minTs);
    }

    public positionToTimestamp(fraction: number): number {
        if (fraction <= 0)
            return this.minTs;
        if (fraction >= 1)
            return this.maxTs;
        return this.minTs + fraction * (this.maxTs - this.minTs);
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
        for (const bar of this.bars) {
            removeAllChildren(bar);
        }
        let firstRow = null;
        if (this.nextKList != null && this.nextKList.rows.length > 0)
            firstRow = this.nextKList.rows[0].values;
        const rr = this.createNextKRequest(this.order, firstRow, this.nextKList.rows.length, null, null);
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

    // get data before the currently visible rows.
    public getBefore(): void {
        const reverseOrder = this.order.invert();
        const rr = this.createNextKRequest(reverseOrder, this.nextKList.rows[0].values ?? null,
            LogFileView.increment, null, null);
        const rec = new LogExpander(this.page, this, rr, this.nextKList, true);
        rr.invoke(rec);
    }

    // get data after the currently visible rows
    public getAfter(): void {
        const rr = this.createNextKRequest(this.order, last(this.nextKList.rows)?.values ?? null,
            LogFileView.increment, null, null);
        const rec = new LogExpander(this.page, this, rr, this.nextKList, false);
        rr.invoke(rec);
    }

    protected showLine(timestamp: number): void {
        const fraction = this.timestampPosition(timestamp);
        this.linePointer.style.top = fractionToPercent(fraction);
        this.linePointer.style.bottom = fractionToPercent(1 - fraction);
    }

    public timestampToString(val: number): string {
        return Converters.valueToString(val, this.timestampColumn.kind, true);
    }

    public updateView(nextKList: NextKList,
                      findResult: FindResult | null): void {
        this.nextKList = nextKList;
        removeAllChildren(this.contents);
        if (nextKList.startPosition > 0) {
            const before = document.createElement("button");
            before.className = "logGap";
            this.contents.appendChild(before);
            before.innerText = formatNumber(nextKList.startPosition) + " rows before; click to bring more";
            before.onclick = () => this.getBefore();
        }
        let rowsShown = 0;
        for (const row of nextKList.rows) {
            rowsShown += row.count;
            const rowSpan = document.createElement("span");
            rowSpan.className = "logRow";
            let index = 0;
            for (const value of row.values) {
                const col = this.order.sortOrientationList[index++];
                const name = col.columnDescription.name;
                if (!this.visibleColumns.has(name))
                    continue;
                if (value == null)
                    continue

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

                high.oncontextmenu = (e) => {
                    this.contextMenu.clear();
                    // This menu shows the value to the right, but the filter
                    // takes the value to the left, so we have to flip all
                    // comparison signs.
                    this.contextMenu.addItem({text: "Keep " + shownValue,
                        action: () => this.filterOnValue(col.columnDescription, value, "=="),
                        help: "Keep only the rows that have this value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep different from " + shownValue,
                        action: () => this.filterOnValue(col.columnDescription, value, "!="),
                        help: "Keep only the rows that have a different value in this column."
                    }, true);
                    this.contextMenu.showAtMouse(e);
                };
            }
            rowSpan.appendChild(document.createElement("br"));
            this.contents.appendChild(rowSpan);
            rowSpan.onmouseover = () => this.showLine(row.values[this.tsIndex] as number);
        }
        if (this.hasData()) {
            const minVisTs = this.firstVisibleTimestamp()!;
            const maxVisTs = this.lastVisibleTimestamp()!;
            const minFraction = this.timestampPosition(minVisTs);
            const maxFraction = this.timestampPosition(maxVisTs);
            this.box.style.top = fractionToPercent(minFraction);
            this.box.style.bottom = fractionToPercent(1 - maxFraction);
            this.summary!.set("Lines visible", rowsShown);
            this.summary!.set("total lines", nextKList.rowsScanned);
            this.summary!.setString("first timestamp", new HtmlString(
                this.timestampToString(this.minTs)));
            this.summary!.setString("last timestamp", new HtmlString(
                this.timestampToString(this.maxTs)));
            this.summary!.setString("first visible timestamp", new HtmlString(
                this.timestampToString(minVisTs)));
            this.summary!.setString("Last visible timestamp", new HtmlString(
                this.timestampToString(maxVisTs)));
        }
        const rowsAfter = nextKList.rowsScanned - (nextKList.startPosition + rowsShown);
        if (rowsAfter > 0) {
            const after = document.createElement("button");
            after.className = "logGap";
            this.contents.appendChild(after);
            after.innerText = formatNumber(rowsAfter) + " rows after; click to bring more";
            after.onclick = () => this.getAfter();
        }
        this.summary!.display();
    }

    protected filterOnValue(cd: IColumnDescription, value: RowValue, comparison: Comparison): void {
        const filter = createComparisonFilter(cd, value, comparison);
        if (filter == null)
            // Some error occurred
            return;
        const rr = this.createFilterComparisonRequest(filter);
        rr.invoke(new LogFilteredReceiver(this.page, rr, this, Converters.comparisonFilterDescription(filter)));
    }

    protected firstVisibleTimestamp(): number | null {
        if (!this.hasData())
            return null;
        return this.nextKList.rows[0].values[this.tsIndex] as number;
    }

    protected lastVisibleTimestamp(): number | null {
        if (!this.hasData())
            return null;
        return last(this.nextKList.rows)!.values[this.tsIndex] as number;
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        assert(false);
    }

    public resize(): void {
        this.updateView(this.nextKList, null);
    }

    public updateLineDensity(value: Groups<number>): void {
        this.plots[0].setHistogram(value, this.minTs, this.maxTs, this.timestampColumn.kind);
        this.plots[0].draw();
    }

    setTimestampRange(min: number, max: number): void {
        this.minTs = min;
        this.maxTs = max;
    }

    protected hasData(): boolean {
        return this.nextKList != null && this.nextKList.rows.length > 0;
    }

    private scrollTimestamp(e: MouseEvent) {
        if (!this.hasData())
            return;
        const y = e.offsetY;
        const fraction = y / this.barHolder.clientHeight;
        const ts = this.positionToTimestamp(fraction);
        // Check if we already have this part; then scroll to it, otherwise bring a new log
        const minFraction = this.timestampPosition(this.firstVisibleTimestamp()!);
        const maxFraction = this.timestampPosition(this.lastVisibleTimestamp()!);
        if (fraction >= minFraction && fraction <= maxFraction) {
            // already available
            // find the closest timestamp
            const closestIndex =
                argmin(this.nextKList.rows, r => Math.abs(r.values[this.tsIndex] as number - ts));
            if (closestIndex >= 0)
                this.contents.children[closestIndex].scrollIntoView();
            return;
        }
        // download it
        const firstRow = [];
        const colsMinValue = [];
        for (const c of this.order.sortOrientationList) {
            if (c.columnDescription.name == this.timestampColumn.name) {
                firstRow.push(ts);
            } else {
                firstRow.push(null);
                colsMinValue.push(c.columnDescription.name);
            }
        }
        const rr = this.createNextKRequest(this.order, firstRow, LogFileView.increment, null, colsMinValue);
        rr.invoke(new LogChooser(this.page, this, rr));
    }

    private onMouseMove(e: MouseEvent): void {
        if (!this.hasData())
            return;
        const y = e.offsetY;
        const fraction = y / this.barHolder.clientHeight;
        const ts = this.positionToTimestamp(fraction);
        // this.summary!.setString("Pointing at", new HtmlString(this.timestampToString(ts)));
        // this.summary!.display();
    }

    public addFilteredView(fd: FilteredDataset): void {
        this.filters.push(fd);
        const filterRow = document.createElement("div");
        filterRow.className = "logFilterRow";
        const span = makeSpan("Filter: " + fd.title);
        span.style.flexGrow = "100";
        filterRow.appendChild(span);

        const close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        const index = this.filters.length - 1;
        close.onclick = () => this.removeFilters(index);
        close.title = "Remove this filter and the subsequent ones.";
        filterRow.appendChild(close);

        this.filterHolder.appendChild(filterRow);
    }

    protected removeFilters(startIndex: number): void {
        if (startIndex >= this.filters.length)
            return;
        removeAllChildren(this.filterHolder);
        const filters = cloneArray(this.filters);
        this.filters = [];
        for (let i = 0; i < startIndex; i++)
            this.addFilteredView(filters[i]);
        for (const bar of this.bars) {
            removeAllChildren(bar);
        }
        this.nextKList = filters[startIndex].previousNextK;
        this.refresh();
    }
}

// This receiver is invoked after a filtering operation has been applied to the log.
class LogFilteredReceiver extends OnCompleteReceiver<RemoteObjectId> {
    constructor(page: FullPage, operation: ICancellable<RemoteObjectId>,
                protected view: LogFileView,
                protected filterDescription: string) {
        super(page, operation, "Filter");
    }

    public run(data: RemoteObjectId): void {
        const fd = new FilteredDataset(data, this.view.meta, this.filterDescription, this.view.nextKList);
        this.view.addFilteredView(fd);
        this.view.refresh();
    }
}

// This receiver is invoked after the visible log has been grown backward or forward
class LogExpander extends OnCompleteReceiver<NextKList> {
    constructor(page: FullPage, protected  view: LogFileView,
                operation: ICancellable<NextKList>,
                protected previous: NextKList | null,
                protected reverse: boolean) {
        super(page, operation, "Getting log fragment");
    }

    public run(data: NextKList): void {
        if (this.reverse) {
            data.rows.reverse();
            const result: NextKList = {
                rowsScanned: data.rowsScanned,
                startPosition: data.rowsScanned - data.startPosition - data.rows.length,
                rows: this.previous != null ? data.rows.concat(this.previous.rows) : data.rows,
                aggregates: null
            }
            this.view.updateView(result, null);
        } else {
            const result: NextKList = {
                rowsScanned: data.rowsScanned,
                startPosition: this.previous != null ? this.previous.startPosition : data.startPosition,
                rows: this.previous != null ? this.previous.rows.concat(data.rows) : data.rows,
                aggregates: null
            }
            this.view.updateView(result, null);
        }
    }
}

class LogChooser extends LogExpander {
    constructor(page: FullPage, protected  view: LogFileView,
                operation: ICancellable<NextKList>) {
        super(page, view, operation, null, false);
    }
}

export class LogFragmentReceiver extends OnCompleteReceiver<NextKList> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<NextKList>) {
        super(page, operation, "Getting log fragment");
    }

    public run(value: NextKList): void {
        this.view.createSurfaces();
        if (value.rowsScanned == 0) {
            this.view.page.reportError("No data left");
            this.view.updateView(value, null);
            this.view.updateLineDensity({ perBucket: [], perMissing: 0 });
            return;
        }
        const rr = this.view.createDataQuantilesRequest([this.view.timestampColumn], this.page, "LogFile");
        rr.chain(this.operation);
        const rec = new TimestampRangeReceiver(this.page, this.view, rr, value);
        rr.invoke(rec);
    }
}

export class TimestampRangeReceiver extends OnCompleteReceiver<BucketsInfo[]> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<BucketsInfo[]>,
                protected initialData: NextKList) {
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
            Math.min(this.initialData.rowsScanned, Math.floor(pixels)),
            true,
            // This is sideways
            { height: this.view.heatmapWidth, width: pixels });
        this.view.setTimestampRange(range.min!, range.max!);
        // must do this after we set the timestamp range
        this.view.updateView(this.initialData, null);
        const rr = this.view.createHistogramRequest({
            histos: [ args ],
            samplingRate: 1.0,
            seed: 0,
        });
        rr.chain(this.operation);
        const rec = new TimestampHistogramReceiver(this.page, this.view, rr);
        rr.invoke(rec);
    }
}

export class TimestampHistogramReceiver extends Receiver<Groups<number>> {
    constructor(page: FullPage,
                protected view: LogFileView,
                operation: ICancellable<Groups<number>>) {
        super(page, operation, "Getting time distribution");
    }

    public onNext(value: PartialResult<Groups<number>>): void {
        if (value != null && value.data != null)
            this.view.updateLineDensity(value.data);
    }

    public onCompleted() {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}