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

import {RemoteTableObjectView} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {NextKList, ColumnDescription, RecordOrder, Schema} from "../javaBridge";
import {SubMenu, TopMenu} from "../ui/menu";
import {TabularDisplay} from "../ui/tabularDisplay";
import {TableView} from "./tableView";

/**
 * This class is used to browse through the columns of a table schema
 * and select columns from them.
 */
export class SchemaView extends RemoteTableObjectView {
    protected display: TabularDisplay;

    constructor(remoteObjectId: string,
                protected page: FullPage,
                public schema: Schema,
                private rowCount: number) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");

        this.topLevel = document.createElement("div");
        let subMenu = new SubMenu([
            {text: "Selected columns", action: () => {this.showTable();}}
        ]);
        let menu = new TopMenu([{text: "View", subMenu}]);
        this.page.setMenu(menu);
        this.topLevel.appendChild(document.createElement("br"));

        this.display = new TabularDisplay();
        this.display.setColumns(["#", "Name", "Type", "Allows missing"],
            ["Colunm number", "Column name", "Type of data stored within the column",
            "If this is true then the column can have 'missing' values."]);

        for (let i = 0; i < schema.length; i++)
            this.display.addRow([(i+1).toString(), schema[i].name,
                schema[i].kind.toString(), schema[i].allowMissing.toString()]);
        this.topLevel.appendChild(this.display.getHTMLRepresentation());
    }

    refresh(): void { }

    /**
     * This method returns a Schema comprising of the selected columns.
     */
    private createSchema(): Schema {
        let cds: ColumnDescription[] = [];
        this.display.getSelectedRows().forEach(i => { cds.push(this.schema[i]) });
        return cds;
    }

    /**
     * This method displays the table consisting of only the columns contained in the schema above.
     */
    private showTable(): void {
        let newPage = new FullPage("Table with selected columns", "Table", this.page);
        this.page.insertAfterMe(newPage);
        let tv = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(tv);
        let nkl = new NextKList();
        nkl.schema = this.createSchema();
        nkl.rowCount = this.rowCount;
        tv.updateView(nkl, false, new RecordOrder([]), 0);
        tv.scrollIntoView();
    }
}
