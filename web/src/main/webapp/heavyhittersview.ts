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

import {RemoteTableObjectView} from "./tableData";
import {significantDigits, FullPage, Resolution} from "./ui";
import {ColumnDescription, Schema, RecordOrder} from "./tableData";
import {TableView, TableDataView, TopList, TableOperationCompleted} from "./table";
import {TopMenu, TopSubMenu} from "./menu";
import {DataRange} from "./vis";

// Class that renders a table containing the heavy hitters in sorted
// order of counts. It also displays a menu that gives the option to
// view the results as filtered version of the original
// table. Clicking this option gives a table with the same rows, but
// they are not in sorted order of counts.
export class HeavyHittersView extends RemoteTableObjectView {
    constructor(public data: TopList,
                public page: FullPage,
                public tv: TableView,
                public schema: Schema,
                public order: RecordOrder) {
        super(data.heavyHittersId, page);
        this.topLevel = document.createElement("div");
        let menu = new TopMenu([
            {
            text: "View", subMenu: new TopSubMenu([
                {text: "As Table", action: () => {this.showTable();}}
            ])
        }
        ]);
        this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.topLevel.appendChild(document.createElement("br"));
    }

    refresh(): void{}

    // Method the creates the filtered table.
    public showTable(): void {
        let newPage2 = new FullPage();
        this.page.insertAfterMe(newPage2);
        let rr = this.tv.createRpcRequest("filterHeavy", {
                hittersId: this.data.heavyHittersId,
                schema: this.schema
        });
        rr.invoke(new TableOperationCompleted(newPage2, this.tv, rr, this.order));
    }

    public fill(tdv: TableDataView, elapsedMS: number): void {
        let scroll_div = document.createElement("div");
        scroll_div.style.maxHeight = Resolution.canvasHeight.toString() + "px";
        scroll_div.style.overflowY = "auto";
        scroll_div.style.display =  "inline-block";
        this.topLevel.appendChild(scroll_div);
        let table = document.createElement("table");
        scroll_div.appendChild(table);

        let tHead = table.createTHead();
        let thr = tHead.appendChild(document.createElement("tr"));
        let thd0 = document.createElement("th");
        thd0.innerHTML = "Rank";

        thr.appendChild(thd0);
        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            let thd = document.createElement("th");
            thd.innerHTML = cd.name;
            thr.appendChild(thd);
        }
        let thd1 = document.createElement("th");
        thd1.innerHTML = "Count";
        thr.appendChild(thd1);
        let thd2 = document.createElement("th");
        thd2.innerHTML = "%";
        thr.appendChild(thd2);
        let thd3 = document.createElement("th");
        thd3.innerHTML = "Fraction";
        thr.appendChild(thd3);

        let restCount = this.getRestCount(tdv);
        let restPos: number;
        if(restCount > 0)
            restPos = this.getRestPos(tdv, restCount);
        else
            restPos = tdv.rows.length;

        let tBody = table.createTBody();
        if (tdv.rows != null) {
            let k = 0;
            let position = 0;
            for (let i = 0; i < tdv.rows.length; i++) {
                k++;
                if (i == restPos) {
                    this.showRest(k, position, restCount, tdv.rowCount, tBody);
                    position += restCount;
                    k++;
                }
                let trow = tBody.insertRow();
                let cell = trow.insertCell(0);
                cell.style.textAlign = "right";
                cell.textContent = k.toString();
                for (let j = 0; j < this.schema.length; j++) {
                    let cell = trow.insertCell(j+1);
                    cell.style.textAlign = "right";
                    let value = tdv.rows[i].values[j];
                    if (value == null) {
                        cell.classList.add("missingData");
                        cell.textContent = "missing";
                    } else
                        cell.textContent = TableView.convert(value, this.schema[j].kind);
                }
                let cell1 = trow.insertCell(this.schema.length + 1);
                cell1.style.textAlign = "right";
                cell1.textContent = significantDigits(tdv.rows[i].count);
                let cell2 = trow.insertCell(this.schema.length + 2);
                cell2.style.textAlign = "right";
                cell2.textContent = significantDigits((tdv.rows[i].count/tdv.rowCount)*100);
                let cell3 = trow.insertCell(this.schema.length + 3);
                let dataRange = new DataRange(position, tdv.rows[i].count, tdv.rowCount);
                cell3.appendChild(dataRange.getDOMRepresentation());
                position += tdv.rows[i].count;
            }
            if ((restPos == tdv.rows.length) && (restCount > 0)) {
                k = tdv.rows.length + 1;
                this.showRest(k, position, restCount, tdv.rowCount, tBody);
            }
        }
        let footer = tBody.insertRow();
        let cell = footer.insertCell(0);
        cell.colSpan = this.schema.length + 4;
        cell.className = "footer";

        this.page.reportError("Operation took " + significantDigits(elapsedMS/1000) + " seconds");
    }

    private getRestCount(tdv:TableDataView): number{
        if (tdv.rows == null)
            return tdv.rowCount;
        else {
            let runCount = tdv.rowCount;
            for (let i = 0; i < tdv.rows.length; i++)
                runCount -= tdv.rows[i].count;
            return runCount;
        }
    }

    private getRestPos(tdv:TableDataView, restCount: number): number{
        if (tdv.rows == null)
            return 0;
        else {
            let i = 0;
            while((i < tdv.rows.length) && (restCount <= tdv.rows[i].count))
                i++;
            return i;
        }
    }

    private showRest(k: number, position: number, restCount: number, total: number, tBody: any): void {
        let trow = tBody.insertRow();
        let cell = trow.insertCell(0);
        cell.style.textAlign = "right";
        cell.textContent = k.toString();
        for (let j = 0; j < this.schema.length; j++) {
            let cell = trow.insertCell(j+1);
            cell.style.textAlign = "right";
            cell.textContent = "Everything Else";
            cell.classList.add("missingData");
        }
        let cell1 = trow.insertCell(this.schema.length + 1);
        cell1.style.textAlign = "right";
        cell1.textContent = significantDigits(restCount);
        let cell2 = trow.insertCell(this.schema.length + 2);
        cell2.style.textAlign = "right";
        cell2.textContent = significantDigits((restCount/total)*100);
        let cell3 = trow.insertCell(this.schema.length + 3);
        let dataRange = new DataRange(position, restCount, total);
        cell3.appendChild(dataRange.getDOMRepresentation());
    }
}
