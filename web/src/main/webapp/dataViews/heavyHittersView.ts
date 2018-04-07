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

import {CombineOperators, IColumnDescription, NextKList, RecordOrder, RemoteObjectId, TopList} from "../javaBridge";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {TableOperationCompleted, TableView} from "./tableView";
import {OnCompleteRenderer, RemoteObject} from "../rpc";
import {cloneSet, ICancellable, significantDigits} from "../util";
import {FullPage} from "../ui/fullPage";
import {SpecialChars, textToDiv} from "../ui/ui";
import {DataRange} from "../ui/dataRange";
import {TabularDisplay} from "../ui/tabularDisplay";
import {RemoteTableObjectView} from "../tableTarget";
import {TableViewBase} from "./tableViewBase";
import {Dialog, FieldKind} from "../ui/dialog";

/**
 * This method handles the outcome of the sketch for finding Heavy Hitters.
 */
export class HeavyHittersReceiver extends OnCompleteRenderer<TopList> {
    public constructor(page: FullPage,
                       protected tv: TableViewBase,
                       operation: ICancellable,
                       protected schema: IColumnDescription[],
                       protected order: RecordOrder,
                       protected isApprox: boolean,
                       protected percent: number) {
        super(page, operation, "Frequent Elements");
    }

    run(data: TopList): void {
        let newPage = this.tv.dataset.newPage("Frequent Elements", this.page);
        let hhv = new HeavyHittersView(data, newPage, this.tv, this.schema, this.order, this.isApprox, this.percent);
        newPage.setDataView(hhv);
        hhv.fill(data.top, this.elapsedMilliseconds());
    }
}

/**
 * Class that renders a table containing the heavy hitters in sorted
 * order of counts. It also displays a menu that gives various option to
 * filter and view the results.
 */
export class HeavyHittersView extends RemoteTableObjectView {
    public static min: number = 0.01;
    public static minString: string = "0.01%";
    public static switchToMG: number = 0.9;
    public static maxDisplay: number = 200; //Should match parameter maxDisplay in FreqKList.java

    contextMenu: ContextMenu;
    protected table: TabularDisplay;
    protected restCount: number;
    protected restPos: number;

    constructor(public data: TopList,
                public page: FullPage,
                public tv: TableViewBase,
                public schema: IColumnDescription[],
                public order: RecordOrder,
                private isApprox: boolean,
                public percent: number) {
        super(data.heavyHittersId, page, "HeavyHitters");
        this.topLevel = document.createElement("div");
        this.contextMenu = new ContextMenu(this.topLevel);
        this.table = new TabularDisplay();

        let tableMenu = new SubMenu([]);
        tableMenu.addItem({
                text: "All Frequent Elements as a Table",
                action: () => this.showTable(isApprox),
                help: "Show the data corresponding to all frequent elements as a table."},
            true);
        tableMenu.addItem({
                text: "Selected Frequent Elements as Table",
                action: () => this.showSelected(),
                help: "Filter by the selected frequent elements and show the result as a table."},
            true );

        let modifyMenu = new SubMenu([]);
        modifyMenu.addItem({
                text: "Get exact counts",
                action: () => this.exactCounts(),
                help: "Show the exact frequency of each item."},
            isApprox);
        modifyMenu.addItem( {
            text: "Change the threshold",
            action: () => this.changeThreshold(),
            help: "Change the frequency threshold for an element to be included"
        }, true);

        let menu = new TopMenu([
            { text: "View as Table",  subMenu: tableMenu, help: "Display frequent elements in a table."},
            { text: "Modify",  subMenu: modifyMenu, help: "Change how frequent elements are computed."},
        ]);
        this.page.setMenu(menu);

        let header: string[] = ["Rank"];
        let tips: string[] = ["Position in decreasing order of frequency."];
        this.schema.forEach(c => {
            header.push(c.name);
            tips.push("Column name");
        });
        header = header.concat(["Count", "% (Above " + this.percent.toString() + ")", "Fraction"]);
        tips = tips.concat(["Number of occurrences", "Frequency within the dataset", "Frequency and position within " +
        "the sorted order"]);
        this.table.setColumns(header, tips);
        this.topLevel.appendChild(this.table.getHTMLRepresentation());
    }

    refresh(): void {}
    /**
     * Method the creates the filtered table. If isApprox is true, then there are two steps: we first compute the exact
     * heavy hitters and then use that list to filter the table. If isApprox is false, we compute the table right away.
     */
    public showTable(isApprox: boolean): void {
        if (isApprox) {
            let rr = this.tv.createCheckHeavyRequest(new RemoteObject(this.data.heavyHittersId), this.schema);
            rr.invoke(new HeavyHittersReceiver3(this, rr));
        } else {
            let newPage = this.dataset.newPage("All frequent elements", this.page);
            let rr = this.tv.createStreamingRpcRequest<RemoteObjectId>("filterHeavy", {
                hittersId: this.data.heavyHittersId,
                schema: this.schema
            });
            rr.invoke(new TableOperationCompleted(newPage, this.tv.schema, rr, this.order, this.tv.dataset));
        }
    }

    public showSelected(): void {
        if (this.table.getSelectedRows().size == 0)
            return;
        let newPage = this.dataset.newPage("Selected frequent elements", this.page);
        let rr = this.tv.createStreamingRpcRequest<RemoteObjectId>("filterListHeavy", {
            hittersId: this.data.heavyHittersId,
            schema: this.schema,
            rowIndices: this.getSelectedRows()
        });
        rr.invoke(new TableOperationCompleted(newPage, this.tv.schema, rr, this.order, this.tv.dataset));
    }

    private getSelectedRows(): number[] {
        let sRows: number[] = cloneSet(this.table.getSelectedRows());
        if (this.restPos == -1)
            return sRows;
        else {
            let tRows: Set<number> = new Set<number>();
            for (let j of sRows) {
                if (j < this.restPos)
                    tRows.add(j);
                else if (j > this.restPos)
                    tRows.add(j - 1);
            }
            return cloneSet(tRows);
        }
    }

    public exactCounts(): void {
        let rr = this.tv.createCheckHeavyRequest(new RemoteObject(this.data.heavyHittersId), this.schema);
        rr.invoke(new HeavyHittersReceiver2(this, rr));
    }

    public fill(tdv: NextKList, elapsedMs: number): void {
        if (tdv.rows.length == 0) this.showEmptyDialog();
        else {
            this.setRest(tdv);
            if (tdv.rows != null) {
                let k = 0;
                let position = 0;
                for (let i = 0; i < tdv.rows.length; i++) {
                    k++;
                    if (i == this.restPos) {
                        this.showRest(k, position, this.restCount, tdv.rowCount, this.table);
                        position += this.restCount;
                        k++;
                    }
                    let row: Element[] = [];
                    row.push(textToDiv(k.toString()));
                    for (let j = 0; j < this.schema.length; j++) {
                        let value = tdv.rows[i].values[j];
                        row.push(textToDiv(TableView.convert(value, this.schema[j].kind)));
                    }
                    row.push(textToDiv(this.valueToString(tdv.rows[i].count)));
                    row.push(textToDiv(this.valueToString((tdv.rows[i].count / tdv.rowCount) * 100)));
                    row.push(new DataRange(position, tdv.rows[i].count, tdv.rowCount).getDOMRepresentation());
                    let tRow: HTMLTableRowElement = this.table.addElementRow(row);
                    tRow.oncontextmenu = e => this.clickThenShowContextMenu(tRow, e);
                    position += tdv.rows[i].count;
                }
                if (this.restPos == tdv.rows.length)
                    this.showRest(tdv.rows.length, position, this.restCount, tdv.rowCount, this.table);
            }
            this.table.addFooter();
            this.page.scrollIntoView();
            this.page.reportTime(elapsedMs);
            if (tdv.rows.length >=  HeavyHittersView.maxDisplay)
                HeavyHittersView.showLongDialog(tdv.rows.length);
        }
    }

    private static showLongDialog(total: number): void {
        let longListDialog = new Dialog("Too Many Frequent Elements", "");
        longListDialog.addText("Showing the top " + HeavyHittersView.maxDisplay.toString() +
            " elements out of " + total);
        longListDialog.addText("Use the 'View as Table' menu option to see the entire list");
        longListDialog.setAction(() => {});
        longListDialog.setCacheTitle("longListDialog");
        longListDialog.show();
    }

    private showEmptyDialog(): void {
        let percentDialog = new Dialog("No Frequent Elements", "");
        percentDialog.addText("No elements found with frequency above " + this.percent.toString() + "%.");
        if (this.percent > HeavyHittersView.min) {
            percentDialog.addText("Lower the threshold? Can take any value above " + HeavyHittersView.minString);
            percentDialog.addTextField("newPercent", "Threshold (%)", FieldKind.Double,
                HeavyHittersView.min.toString(),
                "All values that appear in the dataset with a frequency above this value (as a percent) " +
                "will be considered frequent elements.  Must be at least " + HeavyHittersView.minString);
            percentDialog.setAction(() => {
                let newPercent = percentDialog.getFieldValueAsNumber("newPercent");
                if (newPercent != null)
                    this.runWithThreshold(newPercent);
            });
        }
        else
            percentDialog.setAction(() => {});
        percentDialog.setCacheTitle("noHeavyHittersDialog");
        percentDialog.show();
    }

    private clickThenShowContextMenu(tRow: HTMLTableRowElement, e: MouseEvent): void {
        this.table.clickRow(tRow, e);
        if(e.button == 1) return; //If it was in fact a CTRL+ leftClick on a Mac, don't show the context menu.
        let selectedCount = this.table.selectedRows.size();
        this.contextMenu.clear();
        this.contextMenu.addItem({
                text: "Filter selected frequent elements (as table)",
                action: () => this.showSelected(),
                help: "Show a tabular view containing the selected frequent elements." },
            selectedCount > 0);
        this.contextMenu.addItem({
                text: "Filter all frequent elements (as table)",
                action: () => this.showTable(this.isApprox),
                help: "Show a tabular view containing all frequent elements." },
            true);
        this.contextMenu.show(e);
    }

    /**
     * Method to compute the parameters restCount (count for everything else) and restPos (its rank in the List).
     */
    private setRest(tdv: NextKList): void {
        if (tdv.rows == null) {
            this.restCount = tdv.rowCount;
            this.restPos = 0;
        } else {
            let runCount = tdv.rowCount;
            for (let i = 0; i < tdv.rows.length; i++)
                runCount -= tdv.rows[i].count;
            this.restCount = runCount;
            if (this.restCount < (this.percent * tdv.rowCount) / 100)
                this.restPos = -1;
            else {
                let i = 0;
                while ((i < tdv.rows.length) && (this.restCount <= tdv.rows[i].count))
                    i++;
                this.restPos = i;
            }
        }
        if (this.restPos != -1)
            this.table.excludeRow(this.restPos);
    }

    private showRest(k: number, position: number, restCount: number, total: number, table: TabularDisplay): void {
        let row: Element[] = [];
        row.push(textToDiv(k.toString()));
        for (let j = 0; j < this.schema.length; j++) {
            let m = textToDiv("everything else");
            m.classList.add("missingData");
            row.push(m);
        }
        row.push(textToDiv(this.valueToString(restCount)));
        row.push(textToDiv(this.valueToString((restCount / total) * 100)));
        row.push(new DataRange(position, restCount, total).getDOMRepresentation());
        let tRow: HTMLTableRowElement = table.addElementRow(row, false);
        tRow.onclick = e => e.preventDefault();
        tRow.oncontextmenu = e => e.preventDefault();
    }

    private valueToString(n: number): string {
        let str = significantDigits(n);
        if (this.isApprox)
            str = SpecialChars.approx + str;
        return str;
    }

    private runWithThreshold(newPercent: number) {
        let rr = this.tv.createHeavyHittersRequest(this.schema, newPercent,
            this.tv.getTotalRowCount(), HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(this.tv.getPage(), this.tv, rr, this.schema, this.order, true, newPercent));
    }

    private changeThreshold(): void {
        let percentDialog = new Dialog("Change the frequency threshold", "Changes the frequency threshold above which " +
            "elements are considered frequent");
        percentDialog.addText("Enter a percentage between " + HeavyHittersView.minString +" and 100%");
        percentDialog.addTextField("newPercent", "Threshold (%)", FieldKind.Double,
            this.percent.toString(), "All values that appear with a frequency above this value " +
            "(as a percent) will be considered frequent elements.  Must be at least " + HeavyHittersView.minString);
        percentDialog.setAction(() => {
            let newPercent = percentDialog.getFieldValueAsNumber("newPercent");
            if (newPercent != null) this.runWithThreshold(newPercent);
            });
        percentDialog.setCacheTitle("ChangeHeavyHittersDialog");
        percentDialog.show();
    }

    public combine(how: CombineOperators): void {
        // not used
    }
}

/**
 * This class handles the reply of the "checkHeavy" method when the goal is to to compute and display the Exact counts.
  */
export class HeavyHittersReceiver2 extends OnCompleteRenderer<TopList> {
    public constructor(public hhv: HeavyHittersView,
                       public operation: ICancellable) {
        super(hhv.page, operation, "Frequent elements -- exact counts");
    }

    run(newData: TopList): void {
        let newHhv = new HeavyHittersView(newData, this.page, this.hhv.tv, this.hhv.schema, this.hhv.order, false,
            this.hhv.percent);
        this.page.setDataView(newHhv);
        newHhv.fill(newData.top, this.elapsedMilliseconds());
    }
}

/**
 * This class handles the reply of the "checkHeavy" method when the goal is to filter the table, starting from an
 * approximate HeavyHitters sketch. It uses the TopList returned by Check Heavy to filter the table using the
 * "filterHeavy" method.
 * The code is fairly similar to that in showTable().
 */
export class HeavyHittersReceiver3 extends OnCompleteRenderer<TopList> {
    public constructor(public hhv: HeavyHittersView,
                       public operation: ICancellable) {
        super(hhv.page, operation, "Computing exact frequencies");
    }

    run(exactList: TopList): void {
        let newPage = this.hhv.dataset.newPage("Frequent elements", this.hhv.page);
        let rr = this.hhv.tv.createStreamingRpcRequest<RemoteObjectId>("filterHeavy", {
            hittersId: exactList.heavyHittersId,
            schema: this.hhv.schema
        });
        rr.invoke(new TableOperationCompleted(newPage, this.hhv.tv.schema, rr, this.hhv.order,
            this.hhv.tv.dataset));
    }
}
