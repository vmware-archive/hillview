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

import {IColumnDescription, RecordOrder, NextKList, TopList, RemoteObjectId} from "../javaBridge";
import {TopMenu, SubMenu} from "../ui/menu";
import {TableView, TableOperationCompleted} from "./tableView";
import {RemoteObject, OnCompleteRenderer} from "../rpc";
import {significantDigits, ICancellable} from "../util";
import {FullPage} from "../ui/fullPage";
import {SpecialChars, textToDiv} from "../ui/ui";
import {DataRange} from "../ui/dataRange";
import {TabularDisplay} from "../ui/tabularDisplay";
import {RemoteTableObjectView} from "../tableTarget";

/**
 * Class that renders a table containing the heavy hitters in sorted
 * order of counts. It also displays a menu that gives the option to
 * view the results as filtered version of the original
 * table. Clicking this option gives a table with the same rows, but
 *they are not in sorted order of counts.
 */
export class HeavyHittersView extends RemoteTableObjectView {
    constructor(public data: TopList,
                public page: FullPage,
                public tv: TableView,
                public schema: IColumnDescription[],
                public order: RecordOrder,
                private isApprox: boolean) {
        super(data.heavyHittersId, tv.originalTableId, page);
        this.topLevel = document.createElement("div");
        let subMenu = new SubMenu([
            { text: "As Table",
                action: () => {this.showTable();},
                help: "Show the data corresponding to the heavy elements as a tabular view." }
        ]);
        subMenu.addItem({
             text: "Get exact counts",
             action: () => {this.exactCounts(); },
             help: "Show the exact frequency of each item."},
            isApprox
        );
        let menu = new TopMenu([ {text: "View", help: "Change the way the data is displayed.", subMenu} ]);
        this.page.setMenu(menu);
    }

    refresh(): void {}

    // Method the creates the filtered table.
    public showTable(): void {
        let newPage2 = new FullPage("Frequent elements", "HeavyHitters", this.page);
        this.page.insertAfterMe(newPage2);
        let rr = this.tv.createStreamingRpcRequest<RemoteObjectId>("filterHeavy", {
                hittersId: this.data.heavyHittersId,
                schema: this.schema
        });
        rr.invoke(new TableOperationCompleted(newPage2, this.tv, rr, this.order));
    }

    public exactCounts(): void {
        let rr = this.tv.createCheckHeavyRequest(new RemoteObject(this.data.heavyHittersId), this.schema);
        rr.invoke(new HeavyHittersReceiver2(this, this.originalTableId, rr));
    }

    public fill(tdv: NextKList, elapsedMs: number): void {
        let table = new TabularDisplay();
        let header: string[] = ["Rank"];
        let tips: string[] = ["Position in decreasing order of frequency."];
        this.schema.forEach(c => { header.push(c.name); tips.push("Column name"); });
        header = header.concat(["Count", "%", "Fraction"]);
        tips = tips.concat(["Number of occurrences", "Frequency within the dataset", "Frequency and position within the sorted order"]);
        table.setColumns(header, tips);

        let restCount = this.getRestCount(tdv);
        let restPos: number;
        if(restCount > 0)
            restPos = this.getRestPos(tdv, restCount);
        else
            restPos = tdv.rows.length;

        if (tdv.rows != null) {
            let k = 0;
            let position = 0;
            for (let i = 0; i < tdv.rows.length; i++) {
                k++;
                if (i == restPos) {
                    this.showRest(k, position, restCount, tdv.rowCount, table);
                    position += restCount;
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
                table.addElementRow(row);
                position += tdv.rows[i].count;
            }
            if ((restPos == tdv.rows.length) && (restCount > 0)) {
                k = tdv.rows.length + 1;
                this.showRest(k, position, restCount, tdv.rowCount, table);
            }
        }
        table.addFooter();
        this.topLevel.appendChild(table.getHTMLRepresentation());

        this.page.reportTime(elapsedMs);
    }

    private getRestCount(tdv:NextKList): number{
        if (tdv.rows == null)
            return tdv.rowCount;
        else {
            let runCount = tdv.rowCount;
            for (let i = 0; i < tdv.rows.length; i++)
                runCount -= tdv.rows[i].count;
            return runCount;
        }
    }

    private getRestPos(tdv:NextKList, restCount: number): number{
        if (tdv.rows == null)
            return 0;
        else {
            let i = 0;
            while((i < tdv.rows.length) && (restCount <= tdv.rows[i].count))
                i++;
            return i;
        }
    }

    private valueToString(n: number): string {
        let str = significantDigits(n);
        if (this.isApprox)
            str = SpecialChars.approx + str;
        return str;
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
        row.push(textToDiv(this.valueToString((restCount/total)*100)));
        row.push(new DataRange(position, restCount, total).getDOMRepresentation());
        table.addElementRow(row);
    }
}

/**
 * This class handles the reply of the "checkHeavy" method.
  */
export class HeavyHittersReceiver2 extends OnCompleteRenderer<TopList> {
    public constructor(public hhv: HeavyHittersView,
                       protected originalTableId: RemoteObjectId,
                       public operation: ICancellable) {
        super(hhv.page, operation, "Heavy hitters -- exact counts");
    }

    run(newData: TopList): void {
        let newPage = new FullPage("Heavy hitters", "HeavyHitters", this.hhv.page);
        let newHhv = new HeavyHittersView(newData, newPage, this.hhv.tv, this.hhv.schema, this.hhv.order, false);
        newPage.setDataView(newHhv);
        this.page.insertAfterMe(newPage);
        newHhv.fill(newData.top, this.elapsedMilliseconds());
    }
}
