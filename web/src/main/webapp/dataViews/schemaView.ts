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
    NextKList, IColumnDescription, RecordOrder, Schema, RemoteObjectId, allContentsKind, ColumnSortOrientation,
    CombineOperators
} from "../javaBridge";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {TabularDisplay} from "../ui/tabularDisplay";
import {TableView} from "./tableView";
import {Dialog, FieldKind} from "../ui/dialog";
import {TableViewBase} from "./tableViewBase";
import {cloneToSet, significantDigits} from "../util";
import {SchemaClass} from "../schemaClass";

/**
 * This class is used to browse through the columns of a table schema
 * and select columns from them.
 */
export class SchemaView extends TableViewBase {
    protected display: TabularDisplay;
    protected contextMenu: ContextMenu;
    protected summary: HTMLElement;

    constructor(remoteObjectId: RemoteObjectId,
                page: FullPage,
                schema: SchemaClass,
                rowCount: number,
                elapsedMs: number) {
        super(remoteObjectId, page, "Schema");
        this.rowCount = rowCount;
        this.schema = schema;
        this.show();
        this.page.reportTime(elapsedMs);
    }

    show(): void {
        this.topLevel = document.createElement("div");
        this.contextMenu = new ContextMenu(this.topLevel);

        let viewMenu = new SubMenu([{
            text: "Selected columns",
            action: () => this.showTable(),
            help: "Show the data using a tabular view containing the selected columns."
        }]);
        let selectMenu = new SubMenu([{
            text: "By Name",
            action: () => nameDialog.show(),
            help: "Select Columns by name."
        }, {
            text: "By Type",
            action: () => {
                typeDialog.show();
            },
            help: "Select Columns by type."
        }]);
        let menu = new TopMenu([
            this.saveAsMenu(),
            {text: "View", subMenu: viewMenu, help: "Change the way the data is displayed."},
            {text: "Select", subMenu: selectMenu, help: "Select columns based on attributes."},
            this.chartMenu()
        ]);
        this.page.setMenu(menu);
        this.topLevel.appendChild(document.createElement("br"));

        let para = document.createElement("div");
        para.textContent = "Select the columns that you would like to browse";
        this.topLevel.appendChild(para);

        this.display = new TabularDisplay();
        this.display.setColumns(["#", "Name", "Type"],
            ["Column number", "Column name", "Type of data stored within the column"]);

        /* Dialog box for selecting columns based on name */
        let nameDialog = new Dialog("Select by name",
            "Allows selecting/deselecting columns by name using regular expressions");
        nameDialog.addTextField("selected", "Name", FieldKind.String, "",
            "Names of columns to select (regular expressions allowed)");
        let actions: string[] = ["Add", "Remove"];
        nameDialog.addSelectField("action", "Action", actions, "Add",
            "Add to or Remove from current selection");
        nameDialog.setAction(() => {
            let regExp: RegExp = new RegExp(nameDialog.getFieldValue("selected"));
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

        for (let i = 0; i < this.schema.length; i++) {
            let row = this.display.addRow([(i + 1).toString(), this.schema.get(i).name,
                this.schema.get(i).kind.toString()]);
            row.oncontextmenu = e => this.createAndShowContextMenu(e);
        }
        this.topLevel.appendChild(this.display.getHTMLRepresentation());
        this.display.getHTMLRepresentation().setAttribute("overflow-x", "hidden");
        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
        if (this.rowCount != null)
            this.summary.textContent = significantDigits(this.rowCount) + " rows";
        this.page.setDataView(this);
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
            text: "Drop",
            action: () => this.dropColumns(),
            help: "Drop the selected columns from the view." }, true);
        this.contextMenu.addItem({
            text: "Show as table",
            action: () => this.showTable(),
            help: "Show the selected columns in a tabular view." }, true);
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
                let cd = this.schema.find(colName);
                let so: ColumnSortOrientation = {
                    columnDescription: cd, isAscending: true
                };
                this.showFilterDialog(colName, new RecordOrder([so]));
            },
            help : "Eliminate data that matches/does not match a specific value."
        }, selectedCount == 1);
        this.contextMenu.addItem({
            text: "Compare...",
            action: () => {
                let colName = this.getSelectedColNames()[0];
                let cd = this.schema.find(colName);
                let so: ColumnSortOrientation = {
                    columnDescription: cd, isAscending: true
                };
                this.showCompareDialog(colName, new RecordOrder([so]));
            },
            help : "Eliminate data that matches/does not match a specific value."
        }, selectedCount == 1);
        this.contextMenu.addItem({
            text: "Create column...",
            action: () => this.addColumn(new RecordOrder([])),
            help: "Add a new column computed from the selected columns."
        }, true);
        this.contextMenu.addItem({
            text: "Frequent Elements...",
            action: () => this.heavyHittersDialog(),
            help: "Find the values that occur most frequently in the selected columns."
        }, true);
        this.contextMenu.show(e);
    }

    refresh(): void { }

    private dropColumns(): void {
        let selected = cloneToSet(this.getSelectedColNames());
        this.schema = this.schema.filter(c => !selected.has(c.name));
        this.show();
    }

    private nameAction(regExp: RegExp, action: string) {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.schema.get(i).name.match(regExp)) {
                if (action == "Add")
                    this.display.selectedRows.add(i);
                else if (action = "Remove")
                    this.display.selectedRows.delete(i);
            }
        }
    }

    public combine(how: CombineOperators): void {
        // not used
    }

    public getSelectedColCount(): number {
        return this.display.selectedRows.size();
    }

    public getSelectedColNames(): string[] {
        let colNames: string[] = [];
        this.display.selectedRows.getStates().forEach(i => colNames.push(this.schema.get(i).name));
        return colNames;
    }
    /**
     * @param {string} selectedType: A type of column, from ContentsKind.
     * @param {string} action: Either Add or Remove.
     * This method updates the set of selected columns by adding/removing all columns of selectedType.
     */
    private typeAction(selectedType:string, action: string) {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.schema.get(i).kind == selectedType) {
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
        this.display.getSelectedRows().forEach(i => cds.push(this.schema.get(i)));
        return cds;
    }

    /**
     * This method displays the table consisting of only the columns contained in the schema above.
     */
    private showTable(): void {
        let newPage = this.dataset.newPage(this.page.title, this.page);
        let tv = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(tv);
        let nkl: NextKList = {
            schema: this.createSchema(),
            rowCount: this.rowCount,
            startPosition: 0,
            rows: []
        };
        tv.updateView(nkl, false, new RecordOrder([]), null, 0);
    }
}
