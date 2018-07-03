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
import {DistinctStrings} from "./distinctStrings";
import {DataLoaded, getDescription} from "./initialObject";
import {
    CombineOperators,
    IColumnDescription,
    IDistinctStrings,
    RecordOrder,
    RemoteObjectId,
} from "./javaBridge";
import {OnCompleteReceiver, RpcRequest} from "./rpc";
import {SchemaClassSerialization} from "./schemaClass";
import {BigTableView, TableTargetAPI} from "./tableTarget";
import {HillviewToplevel} from "./toplevel";
import {IDataView} from "./ui/dataview";
import {NotifyDialog} from "./ui/dialog";
import {FullPage} from "./ui/fullPage";
import {MenuItem, SubMenu, TopMenuItem} from "./ui/menu";
import {IHtmlElement, ViewKind} from "./ui/ui";
import {assert, EnumIterators, ICancellable, Pair, PartialResult, saveAs} from "./util";

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
    order: RecordOrder;
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
    exact: boolean;
    columnDescription: IColumnDescription;
}

export interface HeatmapSerialization extends IViewSerialization {
    exact: boolean;
    columnDescription0: IColumnDescription;
    columnDescription1: IColumnDescription;
}

export interface Histogram2DSerialization extends HeatmapSerialization {
    relative: boolean;
}

export interface SpectrumSerialization extends IViewSerialization {
    colNames: string[];
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
    private categoryCache: Map<string, DistinctStrings>;
    private selected: BigTableView; // participates in a combine operation
    private selectedPageId: number;  // id of page containing the selected object (if any)
    private readonly topLevel: HTMLElement;
    private pageCounter: number;
    public readonly allPages: FullPage[];

    /**
     * Build a dataset object.
     * @param remoteObjectId  Id of the remote object containing the dataset data.
     * @param name            A name to display for this dataset.
     * @param loaded          A description of the data that was loaded.
     */
    constructor(public readonly remoteObjectId: RemoteObjectId,
                public name: string,
                public readonly loaded: DataLoaded) {
        this.remoteObject = new TableTargetAPI(remoteObjectId);
        this.categoryCache = new Map<string, DistinctStrings>();
        this.selected = null;
        this.pageCounter = 1;
        this.allPages = [];
        this.topLevel = document.createElement("div");
        this.topLevel.className = "dataset";
        HillviewToplevel.instance.addDataset(this);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public rename(name: string): void {
        this.name = name;
    }

    public select(object: BigTableView, pageId: number): void {
        this.selected = object;
        this.selectedPageId = pageId;
    }

    public toString(): string {
        return this.name;
    }

    /**
     * Creates an RPC request which returns the categorical values for the specified
     * columns
     * @param page  Used for reporting errors.
     * @param columns  A list of columns; some of these may not be categorical.
     * @returns An RPC request which when invoked will return an IDistinctStrings for
     *          each column; the non-categorical columns will have nulls in the result.
     */
    public createGetCategoryRequest(page: FullPage, columns: IColumnDescription[]):
        RpcRequest<DistinctStrings[]> {
        const toBring = columns.filter((c) => c.kind === "Category")
            .map((c) => c.name)
            .filter((c) => !this.categoryCache.has(c));
        return new CategoryValuesRequest(this.remoteObjectId, page, columns, toBring);
    }

    /**
     * Check if the selected object can be combined with the specified one,
     * and if so return it.  Otherwise write an error message and return null.
     */
    public getSelected(): Pair<BigTableView, number> {
        return { first: this.selected, second: this.selectedPageId };
    }

    public setDistinctStrings(columnName: string, values: DistinctStrings): void {
        this.categoryCache.set(columnName, values);
    }

    public getDistinctStrings(columnName: string): DistinctStrings {
        if (this.categoryCache.has(columnName))
            return this.categoryCache.get(columnName);
        return new DistinctStrings(null, columnName);
    }

    public combineMenu(ro: BigTableView, pageId: number): TopMenuItem {
        const combineMenu: MenuItem[] = [];
        combineMenu.push({
            text: "Select current",
            action: () => { this.select(ro, pageId); },
            help: "Save the current view; later it can be combined with another view, " +
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
            this.topLevel.appendChild(pageRepresentation);
            this.allPages.push(toInsert);
        } else {
            const index = this.findIndex(after);
            this.allPages.splice(index + 1, 0, toInsert);
            if (index >= this.topLevel.children.length - 1)
                this.topLevel.appendChild(pageRepresentation);
            else
                this.topLevel.insertBefore(pageRepresentation, this.topLevel.children[index + 1]);
        }
    }

    public remove(page: FullPage): void {
        const index = this.findIndex(page);
        this.allPages.splice(index, 1);
        this.topLevel.removeChild(this.topLevel.children[index]);
    }

    public newPage(title: string, sourcePage: FullPage | null): FullPage {
        const num = this.pageCounter++;
        const page = new FullPage(num, title, sourcePage != null ? sourcePage.pageId : null, this);
        this.insertAfter(page, sourcePage);
        return page;
    }

    /**
     * Creates a page when reconstructing a view that has been saved/bookmarked.
     * The newly created page is always inserted at the end.
     */
    public reconstructPage(title: string, pageNo: number, sourcePageNo: number | null): FullPage {
        const page = new FullPage(pageNo, title, sourcePageNo, this);
        if (pageNo >= this.pageCounter)
            this.pageCounter = pageNo + 1;
        this.insertAfter(page, null);
        return page;
    }

    public scrollIntoView(pageId: number): boolean {
        for (const p of this.allPages) {
            if (p.pageId === pageId) {
                p.scrollIntoView();
                return true;
            }
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
        // This is ugly, but circular module dependences make it
        // difficult to place this method in a set of separate classes.
        const vs = obj as IViewSerialization;
        if (vs.pageId == null ||
            vs.remoteObjectId == null ||
            vs.rowCount == null ||
            vs.title == null ||
            vs.viewKind == null)  // sourcePageId can be null
            return false;
        const page = this.reconstructPage(vs.title, vs.pageId, vs.sourcePageId);
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
            case "Trellis":
                // TODO
                break;
            case "HeavyHitters":
                view = HeavyHittersView.reconstruct(vs as HeavyHittersSerialization, page);
                break;
            case "SVD Spectrum":
                view = SpectrumView.reconstruct(vs as SpectrumSerialization, page);
                break;
            case "LAMP":
                // No longer maintained.
            case "Load":
                // These do not need to be reconstructed ever.
            default:
                break;
        }
        if (view != null) {
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
        const rr = this.remoteObject.createGetSchemaRequest();
        const title = getDescription(this.loaded);
        const newPage = this.newPage(title, null);
        rr.invoke(new SchemaReceiver(newPage, rr, this.remoteObject, this, false));
    }

    public saveToFile(): void {
        const ser = this.serialize();
        const str = JSON.stringify(ser);
        const fileName = "savedView.txt";
        saveAs(fileName, str);
        const notify = new NotifyDialog("File has been saved\n" +
            "Look for file " + fileName + " in the browser Downloads folder",
            "File has been saved");
        notify.show();
    }
}

/**
 * Receives categories for a set of columns from an RPC, caches the result,
 * and then invokes an observer passing all the columns that the observer asked for.
 */
class CategoryValuesObserver extends OnCompleteReceiver<IDistinctStrings[]> {
    constructor(page: FullPage, operation: ICancellable,
                protected requestedColumns: IColumnDescription[],
                protected toBringColumns: string[],
                protected observer: OnCompleteReceiver<DistinctStrings[]>) {
        super(page, operation, "Enumerate categories");
    }

    public run(value: IDistinctStrings[]): void {
        // Receive the columns that we asked for and cache the results
        assert(this.toBringColumns.length === value.length);
        for (let i = 0; i < this.toBringColumns.length; i++) {
            const ds = new DistinctStrings(value[i], this.toBringColumns[i]);
            if (ds.truncated)
                this.page.reportError(
                    "Column " + this.requestedColumns[i] + " has too many distinct values for a category");
            this.page.dataset.setDistinctStrings(this.toBringColumns[i], ds);
        }

        // Build the result expected by the observer: all the requested columns
        const result: DistinctStrings[] = [];
        for (const c of this.requestedColumns)
            result.push(this.page.dataset.getDistinctStrings(c.name));

        this.observer.onNext(new PartialResult<DistinctStrings[]>(1, result));
        this.observer.onCompleted();
    }
}

/**
 * Looks like an RpcRequest, but it is more complicated: it intercepts the results
 * and saves them, and then it invokes the observer.
 */
class CategoryValuesRequest extends RpcRequest<IDistinctStrings[]> {
    constructor(object: RemoteObjectId, protected page: FullPage,
                protected requestedColumns: IColumnDescription[],
                protected toBringColumns: string[]) {
        super(object, "uniqueStrings", toBringColumns);
    }

    public invoke(onReply: OnCompleteReceiver<DistinctStrings[]>) {
        const cvo = new CategoryValuesObserver(
            this.page, this, this.requestedColumns, this.toBringColumns, onReply);
        if (this.toBringColumns.length === 0) {
            cvo.onNext(new PartialResult<DistinctStrings[]>(1, []));
            cvo.onCompleted();
        } else {
            super.invoke(cvo);
        }
    }
}
