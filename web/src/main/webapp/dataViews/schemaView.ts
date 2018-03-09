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

import {FullPage} from "../ui/fullPage";
import {
    NextKList, IColumnDescription, RecordOrder, Schema, RemoteObjectId, allContentsKind, ColumnSortOrientation
} from "../javaBridge";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {TabularDisplay} from "../ui/tabularDisplay";
import {TableView} from "./tableView";
import {Dialog, FieldKind} from "../ui/dialog";
import {TableViewBase} from "./tableViewBase";

/**
 * This class is used to browse through the columns of a table schema
 * and select columns from them.
 */
export class SchemaView extends TableViewBase {
    protected display: TabularDisplay;
    protected contextMenu: ContextMenu;

    constructor(remoteObjectId: RemoteObjectId,
                originalTableId: RemoteObjectId,
                page: FullPage,
                schema: Schema,
                private rowCount: number,
                elapsedMs: number) {
        super(remoteObjectId, originalTableId, page);
        this.topLevel = document.createElement("div");
        this.contextMenu = new ContextMenu(this.topLevel);
        this.schema = schema;

        let viewMenu = new SubMenu([
            {text: "Selected columns",
                action: () => {this.showTable();},
                help: "Show the data using a tabular view containing the selected columns."
            }
        ]);
        let selectMenu = new SubMenu([
            {
                text: "By Name",
                action: () => {nameDialog.show();},
                help: "Select Columns by name."
            },
            {
                text: "By Type",
                action: () => {typeDialog.show();},
                help: "Select Columns by type."
            }
        ]);
        let menu = new TopMenu([
            {text: "View", subMenu: viewMenu, help: "Change the way the data is displayed."},
            {text: "Select", subMenu: selectMenu, help: "Select columns based on attributes." },
            this.chartMenu()
            ]);
        this.page.setMenu(menu);
        this.topLevel.appendChild(document.createElement("br"));

        this.display = new TabularDisplay();
        this.display.setColumns(["#", "Name", "Type"],
            ["Column number", "Column name", "Type of data stored within the column"]);

        /* Dialog box for selecting columns based on name*/
        let nameDialog = new Dialog("Select by name",
            "Allows selecting/deselecting columns by name using regular expressions");
        nameDialog.addTextField("selected", "Name", FieldKind.String, "",
            "Names of columns to select (regular expressions allowed)");
        let actions: string[] =  ["Add", "Remove"];
        nameDialog.addSelectField("action", "Action", actions, "Add",
            "Add to or Remove from current selection");
        nameDialog.setAction(() => {
            let regExp: RegExp= new RegExp(nameDialog.getFieldValue("selected"));
            let action: string = nameDialog.getFieldValue("action");
            this.nameAction(regExp, action);
            this.display.highlightSelectedRows();
        });
        this.display.addRightClickHandler("Name", (e: MouseEvent) => {
            e.preventDefault();
            nameDialog.show()
        });

        /* Dialog box for selecting columns based on type*/
        let typeDialog = new Dialog("Select by type", "Allows selecting/deselecting columns based on type");
        typeDialog.addSelectField("selectedType", "Type", allContentsKind, "String",
            "Type of columns you wish to select");
        typeDialog.addSelectField("action", "Action", actions, "Add",
            "Add to or Remove from current selection");
        typeDialog.setCacheTitle("SchemaTypeDialog");
        typeDialog.setAction(() => {
            let selectedType: string = typeDialog.getFieldValue("selectedType");
            let action: string = typeDialog.getFieldValue("action");
            this.typeAction(selectedType, action);
            this.display.highlightSelectedRows();
        });
        this.display.addRightClickHandler("Type", (e: MouseEvent) => {
            e.preventDefault();
            typeDialog.show()
        });

        for (let i = 0; i < schema.length; i++) {
            let row = this.display.addRow([(i + 1).toString(), schema[i].name,
                schema[i].kind.toString()]);
            row.oncontextmenu = e => this.createAndShowContextMenu(e);
        }
        this.topLevel.appendChild(this.display.getHTMLRepresentation());
        this.page.reportTime(elapsedMs);
    }

    createAndShowContextMenu(e: MouseEvent): void {
        if (e.ctrlKey && (e.button == 1)) {
            // Ctrl + click is interpreted as a right-click on macOS.
            // This makes sure it's interpreted as a column click with Ctrl.
            return;
        }
        this.contextMenu.clear();
        let selectedCount = this.display.selectedRows.size();
        this.contextMenu.addItem({
            text: "Show as table",
            action: () => this.showTable(),
            help: "Show the data using a tabular view containing the selected columns." }, true);
        this.contextMenu.addItem({
            text: "Histogram",
            action: () => this.histogram(false),
            help: "Plot the data in the selected columns as a histogram.  Applies to one or two columns only. " +
            "The data cannot be of type String."
        }, selectedCount >= 1 && selectedCount <= 2);
        this.contextMenu.addItem({
            text: "Heatmap",
            action: () => this.heatMap(),
            help: "Plot the data in the selected columns as a heatmap or as a Trellis plot of heatmaps. " +
            "Applies to two or three columns only."
        }, selectedCount >= 2 && selectedCount <= 3);
        this.contextMenu.addItem({
            text: "Estimate distinct elements",
            action: () => this.hLogLog(),
            help: "Compute an estimate of the number of different values that appear in the selected column."
        }, selectedCount == 1);
        this.contextMenu.addItem({
            text: "Filter...",
            action: () => {
                let colName = this.getSelectedColNames()[0];
                let cd = TableView.findColumn(this.schema, colName);
                let so: ColumnSortOrientation = {
                    columnDescription: cd, isAscending: true
                };
                this.equalityFilter(colName, null, true, new RecordOrder([so]), null);
            },
            help : "Eliminate data that matches/does not match a specific value."
        }, selectedCount == 1);
        this.contextMenu.show(e);
    }

    refresh(): void { }

    private nameAction(regExp: RegExp, action: string) {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.schema[i].name.match(regExp)) {
                if (action == "Add")
                    this.display.selectedRows.add(i);
                else if (action = "Remove")
                    this.display.selectedRows.delete(i);
            }
        }
    }

    public getSelectedColCount(): number {
        return this.display.selectedRows.size();
    }

    public getSelectedColNames(): string[] {
        let colNames: string[] = [];
        this.display.selectedRows.getStates().forEach(i => colNames.push(this.schema[i].name));
        return colNames;
    }
    /**
     * @param {string} selectedType: A type of column, from ContentsKind.
     * @param {string} action: Either Add or Remove.
     * This method updates the set of selected columns by adding/removing all columns of selectedType.
     */
    private typeAction(selectedType:string, action: string) {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.schema[i].kind == selectedType) {
                if (action == "Add")
                    this.display.selectedRows.add(i);
                else if (action = "Remove")
                    this.display.selectedRows.delete(i);
            }
        }
    }

    /**
     * This method returns a Schema comprising of the selected columns.
     */
    private createSchema(): Schema {
        let cds: IColumnDescription[] = [];
        this.display.getSelectedRows().forEach(i => { cds.push(this.schema[i]) });
        return cds;
    }

    /**
     * This method displays the table consisting of only the columns contained in the schema above.
     */
    private showTable(): void {
        let newPage = new FullPage("Table with selected columns", "Table", this.page);
        this.page.insertAfterMe(newPage);
        let tv = new TableView(this.remoteObjectId, this.originalTableId, newPage);
        newPage.setDataView(tv);
        let nkl = new NextKList();
        nkl.schema = this.createSchema();
        nkl.rowCount = this.rowCount;
        tv.updateView(nkl, false, new RecordOrder([]), null, 0);
    }
}
