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

import {TopMenu, SubMenu, TopMenuItem} from "./ui/menu";
import {InitialObject} from "./initialObject";
import {RemoteObject, OnCompleteRenderer} from "./rpc";
import {ICancellable} from "./util";
import {IDataView} from "./ui/dataview";
import {ConsoleDisplay} from "./ui/errReporter";
import {FullPage} from "./ui/fullPage";
import {CSVFilesDescription, JdbcConnectionInformation, Status} from "./javaBridge";
import {Dialog, FieldKind} from "./ui/dialog";

/**
 * The load menu is the first menu that is displayed on the screen.
 */
export class LoadMenu extends RemoteObject implements IDataView {
    private top: HTMLElement;
    private menu: TopMenu;
    private console: ConsoleDisplay;

    constructor(protected init: InitialObject, protected page: FullPage, showManagement: boolean) {
        super(init.remoteObjectId);

        this.top = document.createElement("div");
        let items: TopMenuItem[] = [
            { text: "Test datasets", subMenu: new SubMenu([
                { text: "Flights (all)", action: () => init.testDataset(0, this.page) },
                { text: "Flights (subset)", action: () => init.testDataset(1, this.page) },
                { text: "vrOps", action: () => init.testDataset(2, this.page) },
                { text: "MNIST", action: () => init.testDataset(3, this.page) },
                { text: "Image segmentation", action: () => init.testDataset(4, this.page) },
                { text: "Criteo (subset)", action: () => init.testDataset(5, this.page) },
                { text: "Flights (x5)", action: () => init.testDataset(6, this.page) },
                { text: "Flights (x10)", action: () => init.testDataset(7, this.page) },
                { text: "cabs", action: () => init.testDataset(8, this.page) },
            ]) }, {
                text: "Load", subMenu: new SubMenu([
                    { text: "System logs", action: () => init.loadLogs(this.page) },
                    { text: "CSV files", action: () => this.showCSVFileDialog() },
                    { text: "DB tables", action: () => this.showDBDialog() }
            ]) }
        ];
        if (showManagement) {
            /**
             * These are operations supported by the back-end management API.
             * They are mostly for testing, debugging, maintenance and measurement.
             */
            items.push(
                { text: "Manage", subMenu: new SubMenu([
                    { text: "List machines", action: () => this.ping() },
                    { text: "Toggle memoization", action: () => this.command("toggleMemoization") },
                    { text: "Memory use", action: () => this.command("memoryUse") },
                    { text: "Purge memoized", action: () => this.command("purgeMemoization") },
                    { text: "Purge root datasets", action: () => this.command("purgeDatasets") },
                    { text: "Purge leaf datasets", action: () => this.command("purgeLeafDatasets") }
                ])}
            );
        }

        this.menu = new TopMenu(items);
        this.console = new ConsoleDisplay();
        this.page.setMenu(this.menu);
        this.top.appendChild(this.console.getHTMLRepresentation());
    }

    showDBDialog(): void {
        let dialog = new DBDialog();
        dialog.setAction(() => this.init.loadDBTable(dialog.getConnection(), this.page));
        dialog.show();
    }

    showCSVFileDialog(): void {
        let dialog = new CSVFileDialog();
        dialog.setAction(() => this.init.loadCSVFiles(dialog.getFiles(), this.page));
        dialog.show();
    }

    getHTMLRepresentation(): HTMLElement {
        return this.top;
    }

    ping(): void {
        let rr = this.createRpcRequest("ping", null);
        rr.invoke(new PingReceiver(this.page, rr));
    }

    command(command: string): void {
        let rr = this.createRpcRequest(command, null);
        rr.invoke(new CommandReceiver(command, this.page, rr));
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }

    public refresh(): void {}
}

/**
 * Dialog that asks the user which CSV files to load.
 */
class CSVFileDialog extends Dialog {
    constructor() {
        super("Load CSV files", "Loads comma-separated value (CSV) files from all machines that are part of the service.");
        this.addTextField("folder", "Folder", FieldKind.String, "/",
            "Folder on the remote machines where all the CSV files are found.");
        this.addTextField("fileNamePattern", "File name pattern", FieldKind.String, "*.csv",
            "Shell pattern that describes the names of the files to load.");
        this.addTextField("schemaFile", "Schema file (optional)", FieldKind.String, "data.schema",
            "The name of a JSON file that contains the schema of the data (leave empty if no schema file exists).");
        this.addBooleanField("hasHeader", "Header row", false, "True if first row in each file is a header row");
    }

    public getFiles(): CSVFilesDescription {
        return {
            schemaFile: this.getFieldValue("schemaFile"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            hasHeaderRow: this.getBooleanValue("hasHeader"),
            folder: this.getFieldValue("folder")
        }
    }
}

/**
 * Dialog asking the user which DB table to load.
 */
class DBDialog extends Dialog {
    constructor() {
        super("Load DB tables", "Loads one table on each machine that is part of the service.");
        // TODO: this should be a pattern string, based on local worker name.
        this.addSelectField("databaseKind", "Database kind", ["mysql"], "mysql",
            "The kind of database.");
        this.addTextField("host", "Host", FieldKind.String, "localhost",
            "Machine name where database is located; each machine will open a connection to this host");
        this.addTextField("port", "Port", FieldKind.Integer, "3306",
            "Network port to connect to database; 3306 is the port for MySQL.");
        this.addTextField("database", "Database", FieldKind.String, null,
            "Name of database to load.");
        this.addTextField("table", "Table", FieldKind.String, null,
            "The name of the table to load.");
        this.addTextField("user", "User", FieldKind.String, null,
            "The name of the user opening the connection.");
        this.addTextField("password", "Password", FieldKind.Password, null,
            "The password for the user opening the connection.");
    }

    public getConnection(): JdbcConnectionInformation {
        return {
            host: this.getFieldValue("host"),
            port: this.getFieldValueAsNumber("port"),
            database: this.getFieldValue("database"),
            table: this.getFieldValue("table"),
            user: this.getFieldValue("user"),
            password: this.getFieldValue("password"),
            databaseKind: this.getFieldValue("databaseKind")
        }
    }
}

/**
 * This is the main function exposed to the web page, which causes everything
 * to get going.  It creates and displays the menu for loading data.
 */
export function createLoadMenu(showManagement: boolean): void {
    let page = new FullPage("Load", "Load", null);
    page.append();
    let menu = new LoadMenu(InitialObject.instance, page, showManagement);
    page.setDataView(menu);
}

/**
 * Receives the results of a remote management command.
 * @param T  each individual result has this type.
 */
class CommandReceiver extends OnCompleteRenderer<Status[]> {
    public constructor(name: string, page: FullPage, operation: ICancellable) {
        super(page, operation, name);
    }

    toString(s: Status): string {
        let str = s.hostname + "=>";
        if (s.exception == null)
            str += s.result;
        else
            str += s.exception;
        return str;
    }

    run(value: Status[]): void {
        let res = "";
        for (let s of value) {
            if (res != "")
                res += "\n";
            res += this.toString(s);
        }
        this.page.reportError(res);
    }
}

/**
 * Receives and displays the result of the ping command.
 */
class PingReceiver extends OnCompleteRenderer<string[]> {
    public constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "ping");
    }

    run(value: string[]): void {
        this.page.reportError(this.value.toString());
    }
}