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
import {
    FileSetDescription,
    JdbcConnectionInformation,
    Status,
    UIConfig,
    FederatedDatabase,
    DeltaTableDescription
} from "./javaBridge";
import {OnCompleteReceiver, RemoteObject} from "./rpc";
import {Test} from "./test";
import {IDataView} from "./ui/dataview";
import {Dialog, FieldKind} from "./ui/dialog";
import {ErrorDisplay} from "./ui/errReporter";
import {FullPage} from "./ui/fullPage";
import {MenuItem, SubMenu, TopMenu, TopMenuItem} from "./ui/menu";
import {ViewKind} from "./ui/ui";
import {Converters, ICancellable, loadFile, getUUID, disableSuggestions, assertNever} from "./util";
import {HillviewToplevel} from "./toplevel";

/**
 * The load menu is the first menu that is displayed on the screen.
 * This is not really an IDataView, but it is convenient to
 * reuse this interface for embedding into a page.
 */
export class LoadView extends RemoteObject implements IDataView {
    private readonly top: HTMLElement;
    private menu: TopMenu;
    private readonly console: ErrorDisplay;
    private testDatasetsMenu: SubMenu;
    private loadMenu: SubMenu;
    public readonly viewKind: ViewKind;

    constructor(protected init: InitialObject, protected page: FullPage, protected bookmarkFile: string | null) {
        super(init.getRemoteObjectId());
        this.viewKind = "Load";
        this.top = document.createElement("div");
        this.console = new ErrorDisplay();
        this.top.appendChild(this.console.getHTMLRepresentation());
        this.getUIConfig();
        // Check whether the user is trying to visit a bookmark link
        if (bookmarkFile != null) this.openingBookmark(bookmarkFile);
    }

    private getUIConfig(): void {
        const rr = this.createStreamingRpcRequest<UIConfig>("getUIConfig", null);
        const observer = new UIConfigReceiver(this, rr);
        rr.invoke(observer);
    }

    public configReceived(uiconfig: UIConfig): void {
        HillviewToplevel.instance.setUIConfig(uiconfig);
        this.createMenus();
        if (!uiconfig.enableManagement)
            this.showManagement(false);
        if (uiconfig.hideSuggestions)
            disableSuggestions(false);
    }

    public convertToMenu(data: FileSetDescription[]): MenuItem[] {
        const convert: ((d: FileSetDescription) => MenuItem) = (d: FileSetDescription) => {
            return {
                text: d.name || d.fileNamePattern,
                help: d.name || "Data to load",
                action: () => this.init.loadFiles(d, this.page)
            }
        };
        return data.map(convert);
    }

    private createMenus(): void {
        const demoItems: FileSetDescription[] = [];
        HillviewToplevel.instance.uiconfig.defaultFiles?.forEach(
            e => demoItems.push(e));
        this.testDatasetsMenu = new SubMenu(this.convertToMenu(demoItems));
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
            }, {
                text: "Saved view",
                action: () => this.loadSavedDialog(),
                help: "Load a data view that has been saved previously."
            }, {
                text: "CSV files...",
                action: () => {
                    const dialog = new CSVFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of comma-separated value files residing on the worker machines."
            }, {
                text: "JSON files...",
                action: () => {
                    const dialog = new JsonFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of files containing JSON values residing on the worker machines."
            }, {
                text: "Parquet files...",
                action: () => {
                    const dialog = new ParquetFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of Parquet files residing on the worker machines."
            }, {
                text: "ORC files...",
                action: () => {
                    const dialog = new OrcFileDialog();
                    dialog.setAction(() => this.init.loadFiles(dialog.getFiles(), this.page));
                    dialog.show();
                },
                help: "A set of Orc files residing on the worker machines."
            }, {
                text: "Federated DB tables...",
                action: () => {
                    const dialog = new DBDialog(true);
                    dialog.setAction(() =>
                        this.init.loadFederatedDBTable(dialog.getDBConnection(), dialog.getDbKind() , this.page));
                    dialog.show();
                },
                help: "A set of database tables residing in databases on each worker machine."
            }, {
                text: "Delta table...",
                action: () => {
                    const dialog = new DeltaTableDialog();
                    dialog.setAction(() => this.init.loadDeltaTable(dialog.getDeltaTableDescription(), this.page))
                    dialog.show();
                },
                help: "A delta lake table"
            });
        if (HillviewToplevel.instance.uiconfig.localDbMenu)
            // This is only used for testing differentially-private loading from a
            // local database.
            loadMenuItems.push({
                text: "Local DB table...",
                action: () => {
                    const dialog = new DBDialog(false);
                    dialog.setAction(() => this.init.loadSimpleDBTable(dialog.getJdbcConnection(), this.page));
                    dialog.show();
                },
                help: "A database table in a single database."
            });
        this.loadMenu = new SubMenu(loadMenuItems);
        const items: TopMenuItem[] = [];
        if (!HillviewToplevel.instance.uiconfig.hideDemoMenu)
            items.push({ text: "Demo datasets", help: "Hardwired datasets for testing Hillview.",
                subMenu: this.testDatasetsMenu,
            });
        items.push({
                text: "Load", help: "Load data from the worker machines.",
                subMenu: this.loadMenu });
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
        HillviewToplevel.instance.uiconfig.enableManagement = !HillviewToplevel.instance.uiconfig.enableManagement;
        this.showManagement(HillviewToplevel.instance.uiconfig.enableManagement);
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
            if (files == null || files.length !== 1) {
                this.page.reportError("Please select exactly one file to load");
                return;
            }
            loadFile(files[0], (data) => this.loaded(data, name), this.page.getErrorReporter());
        });
        dialog.show();
    }

    public loaded(savedViewJson: string, title?: string): void {
        const json = JSON.parse(savedViewJson);
        if (json == null || json.views == null || json.remoteObjectId == null) {
            this.page.reportError("File could not be parsed");
            return;
        }
        if (title == null)
            title = json.views[0].title;
        this.page.reportError("Reconstructing " + json.views.length + " views");
        const dataset = new DatasetView(json.remoteObjectId, title!, json, this.page);
        const failures = dataset.reconstruct(json);
        if (failures != 0)
            this.page.reportError("Could not reconstruct some views");
    }

    public openingBookmark(bookmarkFile: string): void {
        const rr = this.createStreamingRpcRequest<object>("openingBookmark", bookmarkFile);
        const updateReceiver = new CreateBookmarkContentReceiver(this.page, rr, bookmarkFile, this);
        rr.invoke(updateReceiver);
    }

    public showManagement(show: boolean): void {
        this.menu.enable("Manage", show);
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

    public getPage(): FullPage {
        return this.page;
    }
}

class UIConfigReceiver extends OnCompleteReceiver<UIConfig> {
    public constructor(protected loadMenu: LoadView, operation: ICancellable<UIConfig>) {
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
            schema: null,
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            headerRow: this.getBooleanValue("hasHeader"),
            fileKind: "csv",
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
        const logFormats = ["%{HADOOP}", "%{RFC5424}", "%{VSANTRACE}", "%{NSXT_PROTON}", "%{NSXT_PROXY}",
                            "%{SYSLOG}", "%{BLOCKTRACE}"];
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
            schema: null,
            logFormat: this.getFieldValue("logFormat"),
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            cookie: getUUID(),
            fileKind: "genericlog",
            startTime: Converters.doubleFromDate(this.getDateTimeValue("startTime")),
            endTime: Converters.doubleFromDate(this.getDateTimeValue("endTime")),
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
            schema: null,
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            fileKind: "json",
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
            schema: null,
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            fileKind: "parquet",
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
            schema: null,
            fileNamePattern: this.getFieldValue("fileNamePattern"),
            fileKind: "orc",
        };
    }
}

class DeltaTableDialog extends Dialog {
    constructor() {
        super(
            "Load a Delta Lake table",
            "Load a Delta Lake table from local filesystem, hdfs, or s3"
        );
        const path = this.addTextField(
            "deltaTablePath",
            "Delta table path",
            FieldKind.String,
            "/delta-table",
            "Path to the delta table to load."
        );
        path.required = true;
        this.addTextField(
            "snapshotVersion",
            "Snapshot Version (optional)",
            FieldKind.Integer,
            null,
            "The version of the snapshot to load, if empty the latest snapshot will be loaded."
        );
        this.setCacheTitle("DeltaTableDialog");
    }

    public getDeltaTableDescription(): DeltaTableDescription {
        return {
            path: this.getFieldValue("deltaTablePath"),
            snapshotVersion: this.getFieldValueAsNumber("snapshotVersion")
        };
    }
}

/**
 * Dialog asking the user which DB table to load.
 */
class DBDialog extends Dialog {
    constructor(isFederated: boolean) {
        super("Load DB tables", "Loads data from a parallel or federated database.");
        const arrDB: FederatedDatabase[] = [];
        if (isFederated) arrDB.push("greenplum");
        else arrDB.push("impala");
        arrDB.push("mysql");

        const sel = this.addSelectFieldAsObject(
            "databaseKind", "Database kind", arrDB, (l) => l.toString(),
            "The kind of database.");
        sel.onchange = () => this.dbChanged();
        const host = this.addTextField("host", "Host", FieldKind.String, "localhost",
            "Machine name where database is located; each machine will open a connection to this host");
        host.required = true;
        const port = this.addTextField("port", "Port", FieldKind.Integer, null,
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
        this.dbChanged();
    }

    public getDbKind(): FederatedDatabase | null {
        return this.getFieldValueAsObject<FederatedDatabase>("databaseKind");
    }

    public dbChanged(): void {
        const db = this.getDbKind();
        switch (db) {
            case null:
                break;
            case "mysql":
                this.setFieldValue("port", "3306");
                this.hideInputField("jmxPort");
                this.hideInputField("dbDir");
                break;
            case "impala":
                this.setFieldValue("port", "21050");
                this.hideInputField("jmxPort");
                this.hideInputField("dbDir");
                break;
            case "greenplum":
                this.setFieldValue("port", "5432");
                this.hideInputField("jmxPort");
                this.hideInputField("dbDir");
                break;
            default:
                assertNever(db);
        }
    }

    public getDBConnection(): JdbcConnectionInformation | null {
        const db = this.getDbKind();
        switch (db) {
            case null:
                return null;
            case "mysql":
            case "impala":
            case "greenplum":
                return this.getJdbcConnection();
            default:
                assertNever(db);
        }
    }

    public getJdbcConnection(): JdbcConnectionInformation {
        return {
            host: this.getFieldValue("host"),
            port: this.getFieldValueAsNumber("port") ?? 0,
            database: this.getFieldValue("database"),
            table: this.getFieldValue("table"),
            user: this.getFieldValue("user"),
            password: this.getFieldValue("password"),
            databaseKind: this.getFieldValue("databaseKind"),
            lazyLoading: true,
        };
    }

    public hideInputField(fieldName: string, field?: HTMLElement) {
        const f = field === undefined ? document.getElementById(fieldName) : field;
        if (f != null) {
            this.setFieldValue(fieldName, "");
            f.removeAttribute("required");
            f.setAttribute("disabled", "");
            f.parentElement!.style.display = 'none';
        }
    }

    public showInputField(fieldName: string, field?: HTMLElement){
        const f = field === undefined ? document.getElementById(fieldName) : field;
        if (f != null) {
            f.removeAttribute("disabled");
            f.setAttribute("required", "");
            f.parentElement!.style.display = 'block';
        }
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

class CreateBookmarkContentReceiver extends OnCompleteReceiver<object> {
    loadMenu: LoadView;
    bookmarkID: string;

    public constructor(page: FullPage, operation: ICancellable<object>,
            bookmarkID: string, loadMenu: LoadView) {
        super(page, operation, "open bookmark");
        this.bookmarkID = bookmarkID;
        this.loadMenu = loadMenu;
    }

    public run(value: object): void {
        this.loadMenu.loaded(JSON.stringify(value));
    }
}
