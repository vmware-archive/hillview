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

import {DatasetView} from "./datasetView";
import {InitialObject} from "./initialObject";
import {FileSetDescription, JdbcConnectionInformation, Status, UIConfig} from "./javaBridge";
import {OnCompleteReceiver, RemoteObject} from "./rpc";
import {Test} from "./test";
import {IDataView} from "./ui/dataview";
import {Dialog, FieldKind} from "./ui/dialog";
import {ErrorDisplay} from "./ui/errReporter";
import {FullPage} from "./ui/fullPage";
import {MenuItem, SubMenu, TopMenu, TopMenuItem} from "./ui/menu";
import {ViewKind} from "./ui/ui";
import {Converters, ICancellable, loadFile, getUUID} from "./util";
import {HillviewToplevel} from "./toplevel";

/**
 * The load menu is the first menu that is displayed on the screen.
 * This is not really an IDataView, but it is convenient to
 * reuse this interface for embedding into a page.
 */
export class LoadMenu extends RemoteObject implements IDataView {
    private readonly top: HTMLElement;
    private menu: TopMenu;
    private readonly console: ErrorDisplay;
    private testDatasetsMenu: SubMenu;
    private loadMenu: SubMenu;
    public readonly viewKind: ViewKind;

    constructor(protected init: InitialObject, protected page: FullPage) {
        super(init.remoteObjectId);
        this.viewKind = "Load";
        this.top = document.createElement("div");
        this.console = new ErrorDisplay();
        this.top.appendChild(this.console.getHTMLRepresentation());
        this.getUIConfig();
    }

    public getRemoteObjectId(): string | null {
        return null;
    }

    private getUIConfig(): void {
        const rr = this.createStreamingRpcRequest<UIConfig>("getUIConfig", null);
        const observer = new UIConfigReceiver(this, rr);
        rr.invoke(observer);
    }

    public configReceived(uiconfig: UIConfig): void {
        HillviewToplevel.instance.setUIConfig(uiconfig);
        this.createMenus();
        if (!uiconfig.enableAdvanced)
            this.showAdvanced(false);
    }

    private createMenus(): void {
        const testitems: MenuItem[] = [];
        testitems.push(
            { text: "Flights (15 columns, CSV)",
                action: () => {
                    const files: FileSetDescription = {
                        fileNamePattern: "data/ontime/????_*.csv*",
                        schemaFile: "short.schema",
                        headerRow: true,
                        repeat: 1,
                        name: "Flights (15 columns)",
                        fileKind: "csv",
                        logFormat: null,
                        startTime: null,
                        endTime: null
                    };
                    this.init.loadFiles(files, this.page);
                },
                help: "The US flights dataset.",
            },
            { text: "Flights (15 columns, ORC)",
                action: () => {
                    const files: FileSetDescription = {
                        fileNamePattern: "data/ontime_small_orc/*.orc",
                        schemaFile: "schema",
                        headerRow: true,
                        repeat: 1,
                        name: "Flights (15 columns, ORC)",
                        fileKind: "orc",
                        logFormat: null,
                        startTime: null,
                        endTime: null
                    };
                    this.init.loadFiles(files, this.page);
                },
                help: "The US flights dataset.",
            },
            { text: "Flights (all columns, ORC)",
                action: () => {
                    const files: FileSetDescription = {
                        fileNamePattern: "data/ontime_big_orc/*.orc",
                        schemaFile: "schema",
                        headerRow: true,
                        repeat: 1,
                        name: "Flights (ORC)",
                        fileKind: "orc",
                        logFormat: null,
                        startTime: null,
                        endTime: null
                    };
                    this.init.loadFiles(files, this.page);
                },
                help: "The US flights dataset -- all 110 columns." });
        if (HillviewToplevel.instance.uiconfig.privateIsCsv) {
            testitems.push({
                text: "Flights (15 columns, CSV, private)",
                action: () => {
                    const files: FileSetDescription = {
                        fileNamePattern: "data/ontime_private/????_*.csv*",
                        schemaFile: "short.schema",
                        headerRow: true,
                        repeat: 1,
                        name: "Flights (private)",
                        fileKind: "csv",
                        logFormat: null,
                        startTime: null,
                        endTime: null
                    };
                    this.init.loadFiles(files, this.page);
                },
                help: "The US flights dataset.",
            });
        } else {
            testitems.push({
                text: "Flights (15 columns, ORC, private)",
                action: () => {
                    const files: FileSetDescription = {
                        fileNamePattern: "data/ontime_private/*.orc",
                        schemaFile: "schema",
                        headerRow: true,
                        repeat: 1,
                        name: "Flights (private)",
                        fileKind: "orc",
                        logFormat: null,
                        startTime: null,
                        endTime: null
                    };
                    this.init.loadFiles(files, this.page);
                },
                help: "The US flights dataset.",
            });
        }
        this.testDatasetsMenu = new SubMenu(testitems);

        const loadMenuItems: MenuItem[] = [];
        loadMenuItems.push({
                text: "Hillview logs",
                    action: () => this.init.loadLogs(this.page),
                    help: "The logs generated by the hillview system itself." },
            {
                text: "Generic logs...",
                action: () => {
                    const dialog = new GenericLogDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of log files residing on the worker machines."
            },
            {
                text: "Syslog...",
                action: () => {
                    const dialog = new GenericLogDialog();
                    dialog.setFieldValue("fileNamePattern", "/var/log/syslog*");
                    dialog.setFieldValue("logFormat", "%{RFC5424}");
                    dialog.setFieldValue("startTime", null);
                    dialog.setFieldValue("endTime", null);
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of log files residing on the worker machines."
            },
            {
                text: "Saved view",
                action: () => this.loadSavedDialog(),
                help: "Load a data view that has been saved previously."
            },
            {
                text: "CSV files...",
                action: () => {
                    const dialog = new CSVFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of comma-separated value files residing on the worker machines."
            },
            {
                text: "JSON files...",
                action: () => {
                    const dialog = new JsonFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of files containing JSON values residing on the worker machines."
            },
            {
                text: "Parquet files...",
                action: () => {
                    const dialog = new ParquetFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of Parquet files residing on the worker machines."
            },
            {
                text: "ORC files...",
                action: () => {
                    const dialog = new OrcFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of Orc files residing on the worker machines."
            },
            {
                text: "Federated DB tables...",
                action: () => {
                    const dialog = new DBDialog();
                    dialog.setAction(() => this.init.loadDBTable(dialog.getConnection(), this.page));
                    dialog.show();
                },
                help: "A set of database tables residing in databases on each worker machine."
            });
        if (HillviewToplevel.instance.uiconfig.localDbMenu)
            loadMenuItems.push({
                text: "Local DB table...",
                action: () => {
                    const dialog = new DBDialog();
                    dialog.setAction(() => this.init.loadSimpleDBTable(dialog.getConnection(), this.page));
                    dialog.show();
                },
                help: "A database table in a single database." });
        this.loadMenu = new SubMenu(loadMenuItems);

        const items: TopMenuItem[] = [
            { text: "Test datasets", help: "Hardwired datasets for testing Hillview.",
                subMenu: this.testDatasetsMenu,
            }, {
                text: "Load", help: "Load data from the worker machines.",
                subMenu: this.loadMenu },
        ];

        if (HillviewToplevel.instance.uiconfig.showTestMenu) {
            items.push({
                text: "Test", help: "Run UI tests", subMenu: new SubMenu([
                    {
                        text: "Run", help: "Run end-to-end tests from the user interface. " +
                            "These tests simulate the user clicking in various menus in the browser." +
                            "The tests must be run " +
                            "immediately after reloading the main web page. The user should " +
                            "not use the mouse during the tests.", action: () => this.runTests(),
                    },
                ]),
            });
        }

        /**
         * These are operations supported by the back-end management API.
         * They are mostly for testing, debugging, maintenance and measurement.
         */
        items.push({
                text: "Manage", help: "Execute cluster management operations.",
                subMenu: new SubMenu([
                    {
                        text: "List machines",
                        action: () => this.ping(),
                        help: "Produces a list of all worker machines.",
                    }, {
                        text: "Set memoization",
                        action: () => this.command("setMemoization"),
                        help: "Asks the workers to memoize query results.",
                    }, {
                        text: "Unset memoization",
                        action: () => this.command("unsetMemoization"),
                        help: "Asks the workers not to memoize query results.",
                    }, {
                        text: "Memory use",
                        action: () => this.command("memoryUse"),
                        help: "Reports Java memory use for each worker.",
                    }, {
                        text: "Purge memoized",
                        action: () => this.command("purgeMemoization"),
                        help: "Remove all memoized datasets from the workers.",
                    }, {
                        text: "Purge root datasets",
                        action: () => this.command("purgeDatasets"),
                        help: "Remove all datasets stored at the root node.",
                    }, {
                        text: "Purge leaf datasets",
                        action: () => this.command("purgeLeafDatasets"),
                        help: "Remove all datasets stored at the worker nodes.",
                    }, {
                        text: "Purge all data",
                        action: () => this.purgeAll(),
                        help: "Purge all data from memory (memoized, root, leaf)"
                    }
                ]),
            },
        );

        this.menu = new TopMenu(items);
        this.page.setMenu(this.menu);
    }

    public purgeAll(): void {
        const rr0 = this.createStreamingRpcRequest<Status[]>("purgeMemoization", null);
        const rr1 = this.createStreamingRpcRequest<Status[]>("purgeDatasets", null);
        const rr2 = this.createStreamingRpcRequest<Status[]>("purgeLeafDatasets", null);
        const rec2 = new GenericReceiver<Status[]>("3", this.page, rr2, () => {});
        const rec1 = new GenericReceiver<Status[]>("2", this.page, rr1, () => { rr2.invoke(rec2); });
        const rec0 = new GenericReceiver<Status[]>("1", this.page, rr0, () => { rr1.invoke(rec1); });
        rr0.invoke(rec0);
    }

    public refresh(): void {
        // Not needed
    }

    public toggleAdvanced(): void {
        // writing it this way will work with undefined values.
        HillviewToplevel.instance.uiconfig.enableAdvanced = !HillviewToplevel.instance.uiconfig.enableAdvanced;
        this.showAdvanced(HillviewToplevel.instance.uiconfig.enableAdvanced);
    }

    public loadSavedDialog(): void {
        const dialog = new Dialog("Load saved view", "Load a view from a file");
        const file = dialog.addFileField("File", "File", "File containing saved view");
        file.required = true;
        const fname = dialog.addTextField("Name", "Tab label", FieldKind.String,
            "Saved", "Name to display for dataset");
        fname.required = true;
        dialog.setAction(() =>  {
            const files = dialog.getFieldValueAsFiles("File");
            const name = dialog.getFieldValue("Name");
            if (files.length !== 1) {
                this.page.reportError("Please select exactly one file to load");
                return;
            }
            loadFile(files[0], (data) => this.loaded(data, name), this.page.getErrorReporter());
        });
        dialog.show();
    }

    public loaded(savedViewJson: string, title: string): void {
        const json = JSON.parse(savedViewJson);
        if (json == null || json.views == null || json.remoteObjectId == null) {
            this.page.reportError("File could not be parsed");
            return;
        }
        this.page.reportError("Reconstructing " + json.views.length + " views");
        const dataset = new DatasetView(json.remoteObjectId, title, json, this.page);
        const success = dataset.reconstruct(json);
        if (!success)
            this.page.reportError("Error reconstructing view");
    }

    public showAdvanced(show: boolean): void {
        this.menu.enable("Manage", show);
        this.loadMenu.enable("Federated DB tables...", show);
    }

    // noinspection JSMethodCanBeStatic
    /**
     * Starts the execution of the UI tests.
     */
    public runTests(): void {
        Test.instance.runTests();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.top;
    }

    public ping(): void {
        const rr = this.createStreamingRpcRequest<string[]>("ping", null);
        rr.invoke(new PingReceiver(this.page, rr));
    }

    public command(command: string): void {
        const rr = this.createStreamingRpcRequest<Status[]>(command, null);
        rr.invoke(new CommandReceiver(command, this.page, rr));
    }

    public resize(): void {}

    public setPage(page: FullPage): void {
        this.page = page;
        page.setDataView(this);
    }

    public getPage(): FullPage {
        return this.page;
    }
}

class UIConfigReceiver extends OnCompleteReceiver<UIConfig> {
    public constructor(protected loadMenu: LoadMenu, operation: ICancellable<UIConfig>) {
        super(loadMenu.getPage(), operation, "get UI config");
    }

    public run(value: UIConfig): void {
        this.loadMenu.configReceived(value);
    }
}

/**
 * Dialog that asks the user which CSV files to load.
 */
class CSVFileDialog extends Dialog {
    constructor() {
        super("Load CSV files",
            "Loads comma-separated value (CSV) files from all machines that are part of the service.");
        const pattern = this.addTextField("fileNamePattern", "File name pattern", FieldKind.String, "/*.csv",
            "Shell pattern that describes the names of the files to load.");
        pattern.required = true;
        this.addTextField("schemaFile", "Schema file (optional)", FieldKind.String, "schema",
            "The name of a JSON file that contains the schema of the data (leave empty if no schema file exists).");
        this.addBooleanField("hasHeader", "Header row", false, "True if first row in each file is a header row");
        this.setCacheTitle("CSVFileDialog");
    }

    public getFiles(): FileSetDescription {
        return {
            schemaFile: this.getFieldValue("schemaFile"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: this.getBooleanValue("hasHeader"),
            repeat: 1,
            name: null,
            fileKind: "csv",
            logFormat: null,
            startTime: null,
            endTime: null
        };
    }
}

class GenericLogDialog extends Dialog {
    constructor() {
        super("Load Generic Logs",
            "Loads log files from all machines that are part of the service.");
        const pattern = this.addTextField("fileNamePattern", "File name pattern(s)", FieldKind.String, "*.log",
            "shell pattern with path that describes the names of the files to load (comma-separated patterns allowed)");
        pattern.required = true;
        // TODO: This should perhaps be read from the back-end service.
        const logFormats = ["%{HADOOP}", "%{RFC5424}", "%{VSANTRACE}", "%{ZOOKEEPERLOG}",
                            "%{YARNLOG}", "%{HBASELOG}", "%{OOZEILOG}", "%{HDFSNAMENODELOG}",
                            "%{HDFSDATANODELOG}", "%{SYSLOG}", "%{BLOCKTRACE}"];
        const format = this.addSelectField("logFormat", "Log format", logFormats, "%{SYSLOG}",
            "Log format : https://github.com/vmware/hillview/blob/master/docs/userManual.md" +
            "#232-specifying-rules-for-parsing-logs");
        format.required = true;
        this.addDateTimeField("startTime", "Start time", new Date(Date.now() - 30 * 60 * 100),
            "Log records prior to this date will be ignored");
        this.addDateTimeField("endTime", "End time", new Date(),
            "Log records after this date will be ignored");
        this.setCacheTitle("GenericLogDialog");
    }

    public getFiles(): FileSetDescription {
        return {
            schemaFile: null,
            logFormat: this.getFieldValue("logFormat"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: false,
            repeat: 1,
            name: null,
            cookie: getUUID(),
            fileKind: "genericlog",
            startTime: Converters.doubleFromDate(this.getDateTimeValue("startTime")),
            endTime: Converters.doubleFromDate(this.getDateTimeValue("endTime"))
        };
    }
}

/**
 * Dialog that asks the user which Json files to load.
 */
class JsonFileDialog extends Dialog {
    constructor() {
        super("Load JSON files", "Loads JSON files from all machines that are part of the service.  Each file should " +
            "contain a JSON array of JSON objects.  All JSON objects should have the same schema.  Each JSON object" +
            "field becomes a separate column.  The schema of all JSON files loaded should be the same.");
        const pattern = this.addTextField("fileNamePattern", "File name pattern", FieldKind.String, "/*.json",
            "Shell pattern that describes the names of the files to load.");
        pattern.required = true;
        this.addTextField("schemaFile", "Schema file (optional)", FieldKind.String, "data.schema",
            "The name of a JSON file that contains the schema of the data (leave empty if no schema file exists).");
        this.setCacheTitle("JsonFileDialog");
    }

    public getFiles(): FileSetDescription {
        return {
            schemaFile: this.getFieldValue("schemaFile"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: false,
            repeat: 1,
            name: null,
            fileKind: "json",
            logFormat: null,
            startTime: null,
            endTime: null
        };
    }
}

/**
 * Dialog that asks the user which Parquet files to load.
 */
class ParquetFileDialog extends Dialog {
    constructor() {
        super("Load Parquet files", "Loads Parquet files from all machines that are part of the service." +
            "The schema of all Parquet files loaded should be the same.");
        const pattern = this.addTextField("fileNamePattern", "File name pattern", FieldKind.String, "/*.parquet",
            "Shell pattern that describes the names of the files to load.");
        pattern.required = true;
        this.setCacheTitle("ParquetFileDialog");
    }

    public getFiles(): FileSetDescription {
        return {
            schemaFile: null,  // not used
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: false,  // not used
            repeat: 1,
            name: null,
            fileKind: "parquet",
            logFormat: null,
            startTime: null,
            endTime: null
        };
    }
}

/**
 * Dialog that asks the user which Orc files to load.
 */
class OrcFileDialog extends Dialog {
    constructor() {
        super("Load ORC files", "Loads ORC files from all machines that are part of the service." +
            "The schema of all ORC files loaded should be the same.");
        const pattern = this.addTextField("fileNamePattern", "File name pattern", FieldKind.String, "/*.orc",
            "Shell pattern that describes the names of the files to load.");
        pattern.required = true;
        this.addTextField("schemaFile", "Schema file (optional)", FieldKind.String, "schema",
            "The name of a JSON file that contains the schema of the data " +
            "(if empty the ORC file schema will be used).");
        this.setCacheTitle("OrcFileDialog");
    }

    public getFiles(): FileSetDescription {
        return {
            schemaFile: this.getFieldValue("schemaFile"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: false,  // not used
            repeat: 1,
            name: null,
            fileKind: "orc",
            logFormat: null,
            startTime: null,
            endTime: null
        };
    }
}

/**
 * Dialog asking the user which DB table to load.
 */
class DBDialog extends Dialog {
    constructor() {
        super("Load DB tables", "Loads one table on each machine that is part of the service.");
        const sel = this.addSelectField("databaseKind", "Database kind", ["mysql", "impala"], "mysql",
            "The kind of database.");
        sel.onchange = () => this.dbChanged();
        const host = this.addTextField("host", "Host", FieldKind.String, "localhost",
            "Machine name where database is located; each machine will open a connection to this host");
        host.required = true;
        const port = this.addTextField("port", "Port", FieldKind.Integer, "3306",
            "Network port to connect to database.");
        port.required = true;
        const database = this.addTextField("database", "Database", FieldKind.String, null,
            "Name of database to load.");
        database.required = true;
        const table = this.addTextField("table", "Table", FieldKind.String, null,
            "The name of the table to load.");
        table.required = true;
        this.addTextField("user", "User", FieldKind.String, null,
            "(Optional) The name of the user opening the connection.");
        this.addTextField("password", "Password", FieldKind.Password, null,
            "(Optional) The password for the user opening the connection.");
        this.setCacheTitle("DBDialog");
    }

    public dbChanged(): void {
        const db = this.getFieldValue("databaseKind");
        switch (db) {
            case "mysql":
                this.setFieldValue("port", "3306");
                break;
            case "impala":
                this.setFieldValue("port", "21050");
                break;
        }
    }

    public getConnection(): JdbcConnectionInformation {
        return {
            host: this.getFieldValue("host"),
            port: this.getFieldValueAsNumber("port"),
            database: this.getFieldValue("database"),
            table: this.getFieldValue("table"),
            user: this.getFieldValue("user"),
            password: this.getFieldValue("password"),
            databaseKind: this.getFieldValue("databaseKind"),
            lazyLoading: true,
        };
    }
}

class GenericReceiver<T> extends OnCompleteReceiver<T> {
    public constructor(name: string, page: FullPage,
                       operation: ICancellable<T>, protected continuation: (t: T) => void) {
        super(page, operation, name);
    }

    public run(value: T): void {
        this.continuation(value);
    }
}

/**
 * Receives the results of a remote management command.
 * @param T  each individual result has this type.
 */
class CommandReceiver extends OnCompleteReceiver<Status[]> {
    public constructor(name: string, page: FullPage, operation: ICancellable<Status[]>) {
        super(page, operation, name);
    }

    public toString(s: Status): string {
        let str = s.hostname + "=>";
        if (s.exception == null)
            str += s.result;
        else
            str += s.exception;
        return str;
    }

    public run(value: Status[]): void {
        let res = "";
        for (const s of value) {
            if (res !== "")
                res += "\n";
            res += this.toString(s);
        }
        this.page.reportError(res);
    }
}

/**
 * Receives and displays the result of the ping command.
 */
class PingReceiver extends OnCompleteReceiver<string[]> {
    public constructor(page: FullPage, operation: ICancellable<string[]>) {
        super(page, operation, "ping");
    }

    public run(value: string[]): void {
        this.page.reportError(value.toString());
    }
}
