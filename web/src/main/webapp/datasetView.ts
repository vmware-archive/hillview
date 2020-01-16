/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import {HeatmapView} from "./dataViews/heatmapView";
import {HeavyHittersView} from "./dataViews/heavyHittersView";
import {Histogram2DView} from "./dataViews/histogram2DView";
import {HistogramView} from "./dataViews/histogramView";
import {SchemaView} from "./dataViews/schemaView";
import {SpectrumView} from "./dataViews/spectrumView";
import {SchemaReceiver, TableView} from "./dataViews/tableView";
import {DataLoaded, getDescription} from "./initialObject";
import {
    CombineOperators,
    IColumnDescription, JsonString, PrivacySchema,
    RecordOrder,
    RemoteObjectId,
} from "./javaBridge";
import {SchemaClassSerialization} from "./schemaClass";
import {BigTableView, TableTargetAPI} from "./tableTarget";
import {HillviewToplevel} from "./toplevel";
import {IDataView} from "./ui/dataview";
import {FullPage, PageTitle} from "./ui/fullPage";
import {ContextMenu, MenuItem, SubMenu, TopMenuItem} from "./ui/menu";
import {IHtmlElement, removeAllChildren, ViewKind} from "./ui/ui";
import {assert, cloneArray, EnumIterators, ICancellable, saveAs} from "./util";
import {TrellisHeatmapView} from "./dataViews/trellisHeatmapView";
import {TrellisHistogram2DView} from "./dataViews/trellisHistogram2DView";
import {TrellisHistogramView} from "./dataViews/trellisHistogramView";
import JSONEditor, {JSONEditorOptions} from "jsoneditor";
import {OnCompleteReceiver} from "./rpc";

export interface IViewSerialization {
    viewKind: ViewKind;
    pageId: number;
    sourcePageId: number;
    title: string;
    remoteObjectId: RemoteObjectId;
    rowCount: number;
    schema: SchemaClassSerialization;
}

export interface HeavyHittersSerialization extends IViewSerialization {
    percent: number;
    remoteTableId: string;
    isApprox: boolean;
    columnsShown: IColumnDescription[];
}

export interface TableSerialization extends IViewSerialization {
    order: RecordOrder;
    firstRow: any[];
    tableRowsDesired: number;
}

export interface HistogramSerialization extends IViewSerialization {
    bucketCount: number;
    samplingRate: number;
    isPie: boolean;
    columnDescription: IColumnDescription;
}

export interface HeatmapSerialization extends IViewSerialization {
    samplingRate: number;
    columnDescription0: IColumnDescription;
    columnDescription1: IColumnDescription;
    xBucketCount: number;
    yBucketCount: number;
}

export interface Histogram2DSerialization extends HeatmapSerialization {
    relative: boolean;
}

export interface SpectrumSerialization extends IViewSerialization {
    colNames: string[];
}

export interface TrellisShapeSerialization {
    groupByColumn: IColumnDescription;
    xWindows: number;
    yWindows: number;
    groupByBucketCount: number;
}

export interface TrellisHistogramSerialization extends
    HistogramSerialization, TrellisShapeSerialization {
}

export interface TrellisHistogram2DSerialization extends
    Histogram2DSerialization, TrellisShapeSerialization {
}

export interface TrellisHeatmapSerialization extends
    HeatmapSerialization, TrellisShapeSerialization {
}

export interface IDatasetSerialization {
    kind: "Saved dataset";
    views: IViewSerialization[];
    remoteObjectId: RemoteObjectId;
}

/**
 * A DatasetView holds all information related to a loaded dataset.
 * A DatasetView represents the original (remote) data loaded from some storage medium.
 * A DatasetView will then have many views.
 */
export class DatasetView implements IHtmlElement {
    public readonly remoteObject: TableTargetAPI;
    private selectedPageId: number;  // id of page containing the selected object (if any)
    private readonly topLevel: HTMLElement;
    private readonly pageContainer: HTMLElement;
    private pageCounter: number;
    public readonly allPages: FullPage[];
    public privacySchema: PrivacySchema = null;
    protected privacyEditor: HTMLElement;
    private readonly menu: ContextMenu;

    /**
     * Build a dataset object.
     * @param remoteObjectId  Id of the remote object containing the dataset data.
     * @param name            A name to display for this dataset.
     * @param loaded          A description of the data that was loaded.
     * @param loadMenuPage    The page of the load menu.
     */
    constructor(public readonly remoteObjectId: RemoteObjectId,
                public name: string,
                public readonly loaded: DataLoaded,
                protected loadMenuPage: FullPage) {
        this.remoteObject = new TableTargetAPI(remoteObjectId);
        this.pageCounter = 1;
        this.allPages = [];
        this.topLevel = document.createElement("div");
        this.topLevel.className = "dataset";
        this.privacyEditor = document.createElement("span");
        this.privacyEditor.style.width = "100%";
        this.privacyEditor.style.height = "500px";
        this.privacyEditor.style.display = "none";
        this.topLevel.appendChild(this.privacyEditor);
        this.pageContainer = document.createElement("div");
        this.topLevel.appendChild(this.pageContainer);
        this.topLevel.appendChild(document.createElement("hr"));
        this.menu = new ContextMenu(this.topLevel, [{
            text: "Save this tab to file",
            action: () => this.saveToFile(),
            help: "Save the views in this tab to a local file;\n" +
                "this file can be loaded later using \"Load saved view\".",
        }, {
            text: "Reload original view",
            action: () => this.redisplay(),
            help: "Load again the first view of this dataset.",
        }, {
            text: "Refresh",
            action: () => this.refresh(),
            help: "Refresh all views of the dataset."
        }, {
            text: "Edit privacy policy",
            action: () => this.editPrivacy(),
            help: "For private datasets: opens an editor to edit the privacy parameters."
        }]);
        this.menu.enable("Edit privacy policy", false);
        HillviewToplevel.instance.addDataset(this, this.menu);
    }

    public rebuild(): void {
        const ser = this.serialize();
        const ds = new DatasetView(ser.remoteObjectId, this.name, ser, this.loadMenuPage);
        const success = ds.reconstruct(ser);
        if (!success)
            this.loadMenuPage.reportError("Error recomputing view");
    }

    /**
     * True if the data in this dataset is from a set of log files.
     */
    public isLog(): boolean {
        return this.loaded.kind === "Hillview logs"
            || (this.loaded.kind === "Files" &&
                this.loaded.description.fileKind === "genericlog");
    }

    public getEpsilon(columns: string[]): number {
        const copy = cloneArray(columns);
        copy.sort();
        const key = copy.join("+");
        let eps = this.privacySchema.epsilons[key];
        if (eps == null)
            eps = this.privacySchema.defaultEpsilons[copy.length.toString()];
        if (eps == null)
            eps = this.privacySchema.defaultEpsilon;
        return eps;
    }

    public setEpsilon(columns: string[], epsilon: number): void {
        const copy = cloneArray(columns);
        copy.sort();
        const key = copy.join("+");
        this.privacySchema.epsilons[key] = epsilon;
        this.uploadPrivacy(JSON.stringify(this.privacySchema), false);
    }

    public editPrivacy(): void {
        this.privacyEditor.style.display = "block";
        const span = document.createElement("span");
        span.textContent = "Edit the privacy policy and press Apply to view. Press Save to save to disk. ";
        this.privacyEditor.appendChild(span);
        const done = document.createElement("button");
        done.textContent = "Apply";
        done.title = "Upload the new privacy policy and refresh all views.";
        this.privacyEditor.appendChild(done);

        const cancel = document.createElement("button");
        cancel.textContent = "Cancel";
        cancel.title = "Do not update the privacy policy.";
        this.privacyEditor.appendChild(cancel);

        if (HillviewToplevel.instance.uiconfig.enableAdvanced) {
            const save = document.createElement("button");
            save.textContent = "Save";
            save.title = "Write the privacy policy to disk, overwriting the existing policy on disk.";
            this.privacyEditor.appendChild(save);
            save.onclick = () => {
                try {
                    const json = editor.getText();  // throws when text is invalid
                    this.privacySchema = JSON.parse(json);
                    this.savePrivacy(json);
                    destroy();
                } catch (exception) {
                    this.loadMenuPage.reportError(exception.toString());
                }
            };
        }

        const editOptions: JSONEditorOptions = { mode: "text", mainMenuBar: false, statusBar: false };
        const editor = new JSONEditor(this.privacyEditor, editOptions, "{}");
        editor.set(this.privacySchema);
        const destroy = () => {
            this.privacyEditor.style.display = "none";
            editor.destroy();
            removeAllChildren(this.privacyEditor);
        };
        done.onclick = () => {
            try {
                const json = editor.getText();  // throws when text is invalid
                this.privacySchema = JSON.parse(json);
                this.uploadPrivacy(json, true);
                destroy();
            } catch (exception) {
                this.loadMenuPage.reportError(exception.toString());
            }
        };
        cancel.onclick = () => {
            destroy();
        };
    }

    private uploadPrivacy(json: string, rebuild: boolean): void {
        const js = new JsonString(json);
        const rr = this.remoteObject.createStreamingRpcRequest<string>("changePrivacy", js);
        const updateReceiver = new UploadPrivacyReceiver(this, this.loadMenuPage, rebuild, rr);
        rr.invoke(updateReceiver);
    }

    private savePrivacy(json: string): void {
        const js = new JsonString(json);
        const rr = this.remoteObject.createStreamingRpcRequest<string>("savePrivacy", js);
        const updateReceiver = new UploadPrivacyReceiver(this, this.loadMenuPage, true, rr);
        rr.invoke(updateReceiver);
    }

    public setPrivate(privacySchema: PrivacySchema): void {
        this.privacySchema = privacySchema;
        this.menu.enable("Edit privacy policy", privacySchema != null);
    }

    public isPrivate(): boolean {
        return this.privacySchema != null;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public rename(name: string): void {
        this.name = name;
    }

    public select(pageId: number): void {
        this.selectedPageId = pageId;
    }

    public toString(): string {
        return this.name;
    }

    public getSelected(): number {
        return this.selectedPageId;
    }

    public combineMenu(ro: BigTableView, pageId: number): TopMenuItem {
        const combineMenu: MenuItem[] = [];
        combineMenu.push({
            text: "Select current",
            action: () => { this.select(pageId); },
            help: "Select the current view; later it can be combined with another view, " +
                  "using one of the operations below.",
        });
        combineMenu.push({text: "---", action: null, help: null});
        EnumIterators.getNamesAndValues(CombineOperators)
            .forEach((c) => combineMenu.push({
                text: c.name,
                action: () => { ro.combine(c.value); },
                help: "Combine the rows in the two views using the " + c.value + " operation",
            }));
        return {
            text: "Combine",
            help: "Combine data from two separate views.",
            subMenu: new SubMenu(combineMenu),
        };
    }

    public findIndex(page: FullPage): number {
        const index = this.allPages.indexOf(page);
        if (index < 0)
            throw new Error("Page not found");
        return index;
    }

    /**
     * Insert a page after the specified page.
     * @param {FullPage} toInsert  Page to insert.
     * @param {FullPage} after     Page to insert after; if null insertion is done at the end.
     */
    public insertAfter(toInsert: FullPage, after: FullPage | null): void {
        assert(toInsert !== null);
        const pageRepresentation = toInsert.getHTMLRepresentation();
        if (after == null) {
            this.pageContainer.appendChild(pageRepresentation);
            this.allPages.push(toInsert);
        } else {
            const index = this.findIndex(after);
            this.allPages.splice(index + 1, 0, toInsert);
            if (index >= this.pageContainer.children.length - 1)
                this.pageContainer.appendChild(pageRepresentation);
            else
                this.pageContainer.insertBefore(pageRepresentation,
                    this.pageContainer.children[index + 1]);
        }
    }

    public remove(page: FullPage): void {
        const index = this.findIndex(page);
        this.allPages.splice(index, 1);
        this.pageContainer.removeChild(this.pageContainer.children[index]);
    }

    /**
     * Move a page on the display.
     * @param page  Page to move.
     * @param up    If true move up on the screen.
     */
    public shift(page: FullPage, up: boolean): void {
        const index = this.findIndex(page);
        if (up && index === 0)
            return;
        if (!up && index === this.allPages.length - 1)
            return;
        // Smaller indices are up.
        const newIndex = index + (up ? -1 : +1);
        this.allPages.splice(index, 1);
        this.allPages.splice(newIndex, 0, page);
        this.pageContainer.removeChild(this.pageContainer.children[index]);
        this.pageContainer.insertBefore(page.getHTMLRepresentation(),
            this.pageContainer.children[newIndex]);
        this.scrollIntoView(page.pageId);
    }

    public newPage(title: PageTitle, sourcePage: FullPage | null): FullPage {
        const num = this.pageCounter++;
        const page = new FullPage(num, title, sourcePage != null ? sourcePage.pageId : null, this);
        this.insertAfter(page, sourcePage);
        return page;
    }

    /**
     * Creates a page when reconstructing a view that has been saved/bookmarked.
     * The newly created page is always inserted at the end.
     */
    public reconstructPage(title: PageTitle, pageNo: number, sourcePageNo: number | null): FullPage {
        const page = new FullPage(pageNo, title, sourcePageNo, this);
        if (pageNo >= this.pageCounter)
            this.pageCounter = pageNo + 1;
        this.insertAfter(page, null);
        return page;
    }

    public findPage(pageId: number): FullPage | null {
        for (const p of this.allPages) {
            if (p.pageId === pageId)
                return p;
        }
        return null;
    }

    public scrollIntoView(pageId: number): boolean {
        const p = this.findPage(pageId);
        if (p != null) {
            p.scrollIntoView();
            return true;
        }
        return false;
    }

    public resize(): void {
        for (const p of this.allPages)
            p.onResize();
    }

    /**
     * Reconstruct one view in the dataset.
     * @param {Object} obj  Object which is a serialization of a BigTableView.
     * @returns {boolean}   True if reconstruction succeeds.
     */
    public reconstructView(obj: object): boolean {
        // This is ugly, but circular module dependencies make it
        // difficult to place this method in a set of separate classes.
        const vs = obj as IViewSerialization;
        if (vs.pageId == null ||
            vs.remoteObjectId == null ||
            vs.rowCount == null ||
            vs.title == null ||
            vs.viewKind == null)  // sourcePageId can be null
            return false;
        const page = this.reconstructPage(new PageTitle(vs.title),
            vs.pageId, vs.sourcePageId);
        let view: IDataView = null;
        switch (vs.viewKind) {
            case "Table":
                view = TableView.reconstruct(vs as TableSerialization, page);
                break;
            case "Histogram":
                view = HistogramView.reconstruct(vs as HistogramSerialization, page);
                break;
            case "2DHistogram":
                view = Histogram2DView.reconstruct(vs as Histogram2DSerialization, page);
                break;
            case "Heatmap":
                view = HeatmapView.reconstruct(vs as HeatmapSerialization, page);
                break;
            case "Schema":
                view = SchemaView.reconstruct(vs, page);
                break;
            case "TrellisHistogram":
                view = TrellisHistogramView.reconstruct(vs as TrellisHistogramSerialization, page);
                break;
            case "Trellis2DHistogram":
                view = TrellisHistogram2DView.reconstruct(vs as TrellisHistogram2DSerialization, page);
                break;
            case "TrellisHeatmap":
                view = TrellisHeatmapView.reconstruct(vs as TrellisHeatmapSerialization, page);
                break;
            case "HeavyHitters":
                view = HeavyHittersView.reconstruct(vs as HeavyHittersSerialization, page);
                break;
            case "SVD Spectrum":
                view = SpectrumView.reconstruct(vs as SpectrumSerialization, page);
                break;
            case "Load":
                // These do not need to be reconstructed ever.
            default:
                break;
        }
        if (view != null) {
            view.refresh();
            page.setDataView(view);
            return true;
        }
        return false;
    }

    /**
     * reconstruct a dataset view from serialized information.
     * @param {Object} obj  Serialized description of the dataset read back.
     * @returns {boolean}   True if the reconstruction succeeded.
     */
    public reconstruct(obj: object): boolean {
        const dss = obj as IDatasetSerialization;
        if (dss.views == null)
            return false;
        if (!Array.isArray(dss.views))
            return false;
        for (const v of dss.views)
            if (!this.reconstructView(v))
                return false;
        return true;
    }

    public serialize(): IDatasetSerialization {
        const result: IDatasetSerialization = {
            remoteObjectId: this.remoteObjectId,
            views: [],
            kind: "Saved dataset",
        };
        for (const p of this.allPages) {
            const vs = p.getDataView() as BigTableView;
            if (vs != null)
                result.views.push(vs.serialize());
        }
        return result;
    }

    /**
     * Displays again the original data.
     */
    public redisplay(): void {
        const rr = this.remoteObject.createGetSummaryRequest();
        const title = getDescription(this.loaded);
        const newPage = this.newPage(new PageTitle(title), null);
        rr.invoke(new SchemaReceiver(newPage, rr, this.remoteObject, this, null, null));
    }

    public saveToFile(): void {
        const ser = this.serialize();
        const str = JSON.stringify(ser);
        const fileName = "savedView.txt";
        saveAs(fileName, str);
    }

    public refresh(): void {
        for (const page of  this.allPages) {
            page.getDataView().refresh();
            // TODO: refresh will un-minimize the page
            // but that will only happen when the asynchronous request comes back,
            // so there is no point minimizing it here.
        }
    }
}

class UploadPrivacyReceiver extends OnCompleteReceiver<string> {
    constructor(protected dataset: DatasetView, page: FullPage,
                protected rebuild: boolean, operation: ICancellable<string>) {
        super(page, operation, "upload privacy");
    }

    public run(value: string): void {
        console.log("Privacy policy has been updated.");
        /*
        if (this.rebuild)
            this.dataset.rebuild();
        else
            this.dataset.refresh();
         */
        this.dataset.refresh();
    }
}
