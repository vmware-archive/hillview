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

import {HeavyHittersSerialization, IViewSerialization} from "../datasetView";
import {
    ColumnSortOrientation,
    IColumnDescription,
    NextKList,
    RecordOrder,
    RemoteObjectId,
    TopList
} from "../javaBridge";
import {OnCompleteReceiver, RemoteObject} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseReceiver, BigTableView, TableTargetAPI} from "../tableTarget";
import {DataRangeUI} from "../ui/dataRangeUI";
import {IDataView} from "../ui/dataview";
import {Dialog, FieldKind, NotifyDialog} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {TabularDisplay} from "../ui/tabularDisplay";
import {Resolution, SpecialChars} from "../ui/ui";
import {
    cloneSet,
    ICancellable,
    makeMissing,
    makeSpan,
    significantDigitsHtml,
    Converters
} from "../util";
import {TableOperationCompleted} from "./tableView";

/**
 * This method handles the outcome of the sketch for finding Heavy Hitters.
 */
export class HeavyHittersReceiver extends OnCompleteReceiver<TopList> {
    public constructor(page: FullPage,
                       protected readonly remoteTableObject: TableTargetAPI,
                       operation: ICancellable<TopList>,
                       protected readonly rowCount: number,
                       protected readonly schema: SchemaClass,
                       protected readonly isApprox: boolean,
                       protected readonly percent: number,
                       protected readonly columnsShown: IColumnDescription[],
                       protected readonly reusePage: boolean) {
        super(page, operation, "Frequent Elements");
    }

    public run(data: TopList): void {
        if (data.top.rows.length === 0) this.showEmptyDialog();
        else {
            const names = this.columnsShown.map((c) => this.schema.displayName(c.name)).join(", ");
            let newPage = this.page;
            if (!this.reusePage)
                newPage = this.page.dataset.newPage(
                    new PageTitle("Frequent Elements in " + names), this.page);
            const hhv = new HeavyHittersView(
                data.heavyHittersId, newPage, this.remoteTableObject, this.rowCount, this.schema,
                this.isApprox, this.percent, this.columnsShown);
            newPage.setDataView(hhv);
            hhv.updateView(data.top);
            hhv.page.scrollIntoView();
            hhv.updateCompleted(this.elapsedMilliseconds());
        }
    }

    private showEmptyDialog(): void {
        const percentDialog = new Dialog("No Frequent Elements", "");
        percentDialog.addText("No elements found with frequency above " + this.percent.toString() + "%.");
        if (this.percent > HeavyHittersView.min) {
            percentDialog.addText("Lower the threshold? Can take any value above " + HeavyHittersView.minString);
            const perc = percentDialog.addTextField("newPercent", "Threshold (%)", FieldKind.Double,
                HeavyHittersView.min.toString(),
                "All values that appear in the dataset with a frequency above this value (as a percent) " +
                "will be considered frequent elements.  Must be at least " + HeavyHittersView.minString);
            perc.required = true;
            percentDialog.setAction(() => {
                const newPercent = percentDialog.getFieldValueAsNumber("newPercent");
                if (newPercent != null) {
                    const rr = this.remoteTableObject.createHeavyHittersRequest(
                        this.columnsShown, newPercent,
                        this.rowCount, HeavyHittersView.switchToMG);
                    rr.invoke(new HeavyHittersReceiver(
                        this.page, this.remoteTableObject, rr, this.rowCount, this.schema,
                        true, newPercent, this.columnsShown, false));
                }
            });
        } else
            percentDialog.setAction(() => {});
        percentDialog.setCacheTitle("noHeavyHittersDialog");
        percentDialog.show();
    }

}

/**
 * Class that renders a table containing the heavy hitters in sorted
 * order of counts. It also displays a menu that gives various option to
 * filter and view the results.
 */
export class HeavyHittersView extends BigTableView {
    public static min: number = 0.01;
    public static minString: string = "0.01%";
    public static switchToMG: number = 0.9;
    public static maxDisplay: number = 200; // Should match parameter maxDisplay in FreqKList.java
    public static csBuckets = 500;
    public static csTrials = 50;

    public contextMenu: ContextMenu;
    protected table: TabularDisplay;
    protected restCount: number;
    protected restPos: number;
    private nextKList: NextKList = null;

    constructor(public heavyHittersId: RemoteObjectId,
                public page: FullPage,
                public remoteTableObject: TableTargetAPI,
                rowCount: number,
                schema: SchemaClass,
                private isApprox: boolean,
                public percent: number,
                public readonly columnsShown: IColumnDescription[]) {
        super(heavyHittersId, rowCount, schema, page, "HeavyHitters");
        this.topLevel = document.createElement("div");
        this.contextMenu = new ContextMenu(this.topLevel);
        this.table = new TabularDisplay();

        const tableMenu = new SubMenu([]);
        tableMenu.addItem({
                text: "All Frequent Elements as a Table",
                action: () => this.showTable(isApprox, true),
                help: "Show all frequent elements as a table."},
            true);
        tableMenu.addItem({
                text: "All but the Frequent Elements as a Table",
                action: () => this.showTable(isApprox, false),
                help: "Show all except the frequent elements as a table."},
            true);
        const modifyMenu = new SubMenu([]);
        modifyMenu.addItem({
                text: "Get exact counts",
                action: () => this.exactCounts(),
                help: "Show the exact frequency of each item."},
            isApprox);
        modifyMenu.addItem( {
            text: "Change the threshold",
            action: () => this.changeThreshold(),
            help: "Change the frequency threshold for an element to be included",
        }, true);

        const menu = new TopMenu([
            { text: "View as Table",  subMenu: tableMenu, help: "Display frequent elements in a table."},
            { text: "Modify",  subMenu: modifyMenu, help: "Change how frequent elements are computed."},
        ]);
        this.page.setMenu(menu);

        let header: string[] = ["Rank"];
        let tips: string[] = ["Position in decreasing order of frequency."];
        this.columnsShown.forEach((c) => {
            header.push(this.schema.displayName(c.name).displayName);
            tips.push("Column name");
        });
        header = header.concat(["Count", "% (Above " + this.percent.toString() + ")", "Fraction"]);
        tips = tips.concat(["Number of occurrences", "Frequency within the dataset", "Frequency and position within " +
        "the sorted order"]);
        this.table.setColumns(header, tips);
        this.topLevel.appendChild(this.table.getHTMLRepresentation());
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: HeavyHittersSerialization = {
            ...super.serialize(),
            percent: this.percent,
            remoteTableId: this.remoteTableObject.remoteObjectId,
            isApprox: this.isApprox,
            columnsShown: this.columnsShown,
        };
        return result;
    }

    /**
     * This method is called when all the data has been received.
     */
    public updateCompleted(timeInMs: number): void {
        super.updateCompleted(timeInMs);
        if (this.nextKList != null &&
            this.nextKList.rows != null &&
            this.nextKList.rows.length >= HeavyHittersView.maxDisplay) {
                const longListDialog = new NotifyDialog("Too Many Frequent Elements",
                    "Showing the top " + HeavyHittersView.maxDisplay.toString() +
                    " elements out of " + this.nextKList.rows.length + "\n" +
                    "Use the 'View as Table' menu option to see the entire list",
                    "");
                longListDialog.show();
        }
    }

    public static reconstruct(ser: HeavyHittersSerialization, page: FullPage): IDataView {
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        const percent: number = ser.percent;
        const remoteTableId: string = ser.remoteTableId;
        const isApprox: boolean = ser.isApprox;
        const columnsShown: IColumnDescription[] = ser.columnsShown;
        if (schema == null || percent == null || remoteTableId == null || isApprox == null ||
            columnsShown == null)
            return null;
        const remoteTable = new TableTargetAPI(remoteTableId);
        return new HeavyHittersView(ser.remoteObjectId, page, remoteTable, ser.rowCount, schema,
            isApprox, percent, columnsShown);
    }

    public refresh(): void {
        const rr = this.remoteTableObject.createHeavyHittersRequest(
            this.columnsShown, this.percent, this.rowCount, HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(
            this.getPage(), this, rr, this.rowCount, this.schema,
            this.isApprox, this.percent, this.columnsShown, true));
    }

    public createOrder(): RecordOrder {
        const cso: ColumnSortOrientation[] = [];
        for (const col of this.columnsShown) {
            cso.push({
                columnDescription: col,
                isAscending: true
            });
        }
        return new RecordOrder(cso);
    }

    public resize(): void {}
    /**
     * Method the creates the filtered table. If isApprox is true, then there are two steps: we first compute the exact
     * heavy hitters and then use that list to filter the table. If isApprox is false, we compute the table right away.
     */
    public showTable(isApprox: boolean, includeSet: boolean): void {
        if (isApprox) {
            const rr = this.remoteTableObject.createCheckHeavyRequest(
                new RemoteObject(this.heavyHittersId), this.columnsShown);
            rr.invoke(new HeavyHittersReceiver3(this, rr, includeSet));
        } else {
            const newPage = this.dataset.newPage(
                new PageTitle("All frequent elements"), this.page);
            const rr = this.remoteTableObject.createFilterHeavyRequest(
                this.heavyHittersId, this.columnsShown, includeSet);
            rr.invoke(new TableOperationCompleted(
                newPage, rr, this.rowCount, this.schema,
                this.createOrder(), Resolution.tableRowsOnScreen, null));
        }
    }

    public showSelected(includeSet: boolean): void {
        if (this.table.getSelectedRows().size === 0)
            return;
        const title: string = includeSet ? "Selected frequent elements" : "All other elements";
        const newPage = this.dataset.newPage(new PageTitle(title), this.page);
        const rr = this.remoteTableObject.createFilterListHeavy(
            this.heavyHittersId, this.columnsShown, includeSet, this.getSelectedRows());
        rr.invoke(new TableOperationCompleted(
            newPage, rr, this.rowCount, this.schema, this.createOrder(),
            Resolution.tableRowsOnScreen, null));
    }

    private getSelectedRows(): number[] {
        const sRows: number[] = cloneSet(this.table.getSelectedRows());
        if (this.restPos === -1)
            return sRows;
        else {
            const tRows: Set<number> = new Set<number>();
            for (const j of sRows) {
                if (j < this.restPos)
                    tRows.add(j);
                else if (j > this.restPos)
                    tRows.add(j - 1);
            }
            return cloneSet(tRows);
        }
    }

    public exactCounts(): void {
        const rr = this.remoteTableObject.createCheckHeavyRequest(
            new RemoteObject(this.heavyHittersId), this.columnsShown);
        rr.invoke(new HeavyHittersReceiver2(this, rr));
    }

    public updateView(nextKList: NextKList): void {
        this.nextKList = nextKList;
        this.setRest(nextKList);
        if (nextKList.rows != null) {
            let k = 0;
            let position = 0;
            for (let i = 0; i < nextKList.rows.length; i++) {
                k++;
                if (i === this.restPos) {
                    this.showRest(k, position, this.restCount, nextKList.rowsScanned, this.table);
                    position += this.restCount;
                    k++;
                }
                const row: Element[] = [];
                row.push(makeSpan(k.toString(), false));
                for (let j = 0; j < this.columnsShown.length; j++) {
                    const value = nextKList.rows[i].values[j];
                    if (value == null)
                        row.push(makeMissing());
                    else
                        row.push(makeSpan(Converters.valueToString(
                            value, this.columnsShown[j].kind), false));
                }
                row.push(this.valueToHtml(nextKList.rows[i].count));
                row.push(this.valueToHtml((nextKList.rows[i].count / nextKList.rowsScanned) * 100));
                row.push(new DataRangeUI(position, nextKList.rows[i].count,
                    nextKList.rowsScanned).getDOMRepresentation());
                const tRow: HTMLTableRowElement = this.table.addElementRow(row);
                tRow.oncontextmenu = (e) => this.clickThenShowContextMenu(tRow, e);
                position += nextKList.rows[i].count;
            }
            if (this.restPos === nextKList.rows.length)
                this.showRest(nextKList.rows.length, position, this.restCount, nextKList.rowsScanned, this.table);
        }
        this.table.addFooter();
    }

    private clickThenShowContextMenu(tRow: HTMLTableRowElement, e: MouseEvent): void {
        this.table.clickRow(tRow, e);
        if (e.button === 1) return; // If it was in fact a CTRL+ leftClick on a Mac, don't show the context menu.
        const selectedCount = this.table.selectedRows.size();
        this.contextMenu.clear();
        this.contextMenu.addItem({
                text: "Show selected elements as table",
                action: () => this.showSelected(true),
                help: "Show a tabular view containing the selected frequent elements." },
            selectedCount > 0);
        this.contextMenu.addItem({
                text: "Show all but the selected elements as table",
                action: () => this.showSelected(false),
                help: "Show a tabular view excluding the selected frequent elements." },
            selectedCount > 0);
        this.contextMenu.show(e);
    }

    /**
     * Method to compute the parameters restCount (count for everything else) and restPos (its rank in the List).
     */
    private setRest(tdv: NextKList): void {
        if (tdv.rows == null) {
            this.restCount = tdv.rowsScanned;
            this.restPos = 0;
        } else {
            let runCount = tdv.rowsScanned;
            for (const ri of tdv.rows)
                runCount -= ri.count;
            this.restCount = runCount;
            if (this.restCount < (this.percent * tdv.rowsScanned) / 100)
                this.restPos = -1;
            else {
                let i = 0;
                while ((i < tdv.rows.length) && (this.restCount <= tdv.rows[i].count))
                    i++;
                this.restPos = i;
            }
        }
        if (this.restPos !== -1)
            this.table.excludeRow(this.restPos);
    }

    private showRest(k: number, position: number, restCount: number, total: number, table: TabularDisplay): void {
        const row: Element[] = [];
        row.push(makeSpan(k.toString(), false));
        for (let j = 0; j < this.columnsShown.length; j++) { // tslint:disable-line
            const m = makeSpan("everything else", false);
            m.classList.add("missingData");
            row.push(m);
        }
        row.push(this.valueToHtml(restCount));
        row.push(this.valueToHtml((restCount / total) * 100));
        row.push(new DataRangeUI(position, restCount, total).getDOMRepresentation());
        const tRow: HTMLTableRowElement = table.addElementRow(row, false);
        tRow.onclick = (e) => e.preventDefault();
        tRow.oncontextmenu = (e) => e.preventDefault();
    }

    private valueToHtml(n: number): HTMLElement {
        const span = document.createElement("span");
        let str = significantDigitsHtml(n);
        if (this.isApprox)
            str = str.prependSafeString(SpecialChars.approx);
        str.setInnerHtml(span);
        return span;
    }

    private runWithThreshold(newPercent: number): void {
        const rr = this.remoteTableObject.createHeavyHittersRequest(
            this.columnsShown, newPercent,
            this.rowCount, HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(
            this.getPage(), this.remoteTableObject, rr, this.rowCount, this.schema,
            true, newPercent, this.columnsShown, false));
    }

    private changeThreshold(): void {
        const percentDialog = new Dialog("Change the frequency threshold",
            "Changes the frequency threshold above which elements are considered frequent");
        percentDialog.addText("Enter a percentage between " + HeavyHittersView.minString + " and 100%");
        const perc = percentDialog.addTextField("newPercent", "Threshold (%)", FieldKind.Double,
            this.percent.toString(), "All values that appear with a frequency above this value " +
            "(as a percent) will be considered frequent elements.  Must be at least " + HeavyHittersView.minString);
        perc.min = HeavyHittersView.minString;
        perc.max = "100";
        perc.required = true;
        percentDialog.setAction(() => {
            const newPercent = percentDialog.getFieldValueAsNumber("newPercent");
            if (newPercent != null) this.runWithThreshold(newPercent);
        });
        percentDialog.setCacheTitle("ChangeHeavyHittersDialog");
        percentDialog.show();
    }

    // noinspection JSUnusedLocalSymbols
    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        // Not used
        return null;
    }
}

/**
 * This class handles the reply of the "checkHeavy" method when the goal is to to compute and display the Exact counts.
 */
export class HeavyHittersReceiver2 extends OnCompleteReceiver<TopList> {
    public constructor(public hhv: HeavyHittersView,
                       public operation: ICancellable<TopList>) {
        super(hhv.page, operation, "Frequent elements -- exact counts");
    }

    public run(newData: TopList): void {
        const newHhv = new HeavyHittersView(
            newData.heavyHittersId, this.page, this.hhv.remoteTableObject,
            this.hhv.rowCount, this.hhv.schema, false,
            this.hhv.percent, this.hhv.columnsShown);
        this.page.setDataView(newHhv);
        newHhv.updateView(newData.top);
        newHhv.updateCompleted(this.elapsedMilliseconds());
    }
}

/**
 * This class handles the reply of the "checkHeavy" method when the goal is to filter the table, starting from an
 * approximate HeavyHitters sketch. It uses the TopList returned by Check Heavy to filter the table using the
 * "filterHeavy" method.
 * The code is fairly similar to that in showTable().
 */
export class HeavyHittersReceiver3 extends OnCompleteReceiver<TopList> {
    public constructor(public hhv: HeavyHittersView,
                       public operation: ICancellable<TopList>,
                       public includeSet: boolean
                       ) {
        super(hhv.page, operation, "Computing exact frequencies");
    }

    public run(exactList: TopList): void {
        const title = (this.includeSet) ? "Frequent Elements" : "Infrequent Elements";
        const newPage = this.hhv.dataset.newPage(new PageTitle(title), this.hhv.page);
        const rr = this.hhv.remoteTableObject.createFilterHeavyRequest(
            exactList.heavyHittersId, this.hhv.columnsShown, this.includeSet);
        rr.invoke(new TableOperationCompleted(
            newPage, rr, this.hhv.rowCount, this.hhv.schema, this.hhv.createOrder(),
            Resolution.tableRowsOnScreen, null));
    }
}
