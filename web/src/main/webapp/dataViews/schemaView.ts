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
import {
    NextKList, ColumnDescription, RecordOrder, Schema, RemoteObjectId, allContentsKind
} from "../javaBridge";
import {SubMenu, TopMenu} from "../ui/menu";
import {TabularDisplay} from "../ui/tabularDisplay";
import {TableView} from "./tableView";
import {Dialog, FieldKind} from "../ui/dialog";

/**
 * This class is used to browse through the columns of a table schema
 * and select columns from them.
 */
export class SchemaView extends RemoteTableObjectView {
    protected display: TabularDisplay;

    constructor(remoteObjectId: RemoteObjectId,
                originalTableId: RemoteObjectId,
                protected page: FullPage,
                public schema: Schema,
                private rowCount: number) {
        super(remoteObjectId, originalTableId, page);
        this.topLevel = document.createElement("div");
        let subMenu = new SubMenu([
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
            },
            {
                text: "By Allows Missing",
                action: () => {missingDialog.show();},
                help: "Select Columns by whether missing values are allowed."
            }
        ]);
        let menu = new TopMenu([
            {text: "View", subMenu: subMenu, help: "Change the way the data is displayed."},
            {text: "Select", subMenu: selectMenu, help: "Select columns based on attributes." }
            ]);
        this.page.setMenu(menu);
        this.topLevel.appendChild(document.createElement("br"));

        this.display = new TabularDisplay();
        this.display.setColumns(["#", "Name", "Type", "Allows missing"],
            ["Column number", "Column name", "Type of data stored within the column",
            "If this is true then the column can have 'missing' values."]);

        /* Dialog box for selecting columns based on name*/
        let nameDialog = new Dialog("Select by name", "Allows selecting/deselecting columns by name using regular expressions");
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

        /* Dialog box for selecting columns based on whether they allow missing values.*/
        let missingDialog = new Dialog("Select by Allows Missing", "Allows " +
            "selecting/deselecting columns based on the Allows missing attribute");
        missingDialog.addBooleanField("allowsMissing", "Allows Missing",true,
            "Type of columns you wish to select");
        missingDialog.addSelectField("action", "Action", actions, "Add",
            "Add to or Remove from current selection");
        missingDialog.setCacheTitle("SchemaMissingDialog");
        missingDialog.setAction(() => {
            let missingType: boolean = missingDialog.getBooleanValue("allowsMissing");
            let action: string = missingDialog.getFieldValue("action");
            this.missingAction(missingType, action);
            this.display.highlightSelectedRows();
        });
        this.display.addRightClickHandler("Allows missing", (e: MouseEvent) => {
            e.preventDefault();
            missingDialog.show()
        });

        for (let i = 0; i < schema.length; i++)
            this.display.addRow([(i+1).toString(), schema[i].name,
                schema[i].kind.toString(), schema[i].allowMissing.toString()]);
        this.topLevel.appendChild(this.display.getHTMLRepresentation());
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
     *
     * This method updates the set of selected columns by adding/removing all columns with a given value of
     * AllowsMissing (either true of false).
     */
    private missingAction(missingType: boolean, action: string) {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.schema[i].allowMissing == missingType) {
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
        let tv = new TableView(this.remoteObjectId, this.originalTableId, newPage);
        newPage.setDataView(tv);
        let nkl = new NextKList();
        nkl.schema = this.createSchema();
        nkl.rowCount = this.rowCount;
        tv.updateView(nkl, false, new RecordOrder([]), 0);
    }
}
