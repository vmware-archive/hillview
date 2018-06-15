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

import {CombineOperators, IColumnDescription, IDistinctStrings, RemoteObjectId} from "./javaBridge";
import {OnCompleteReceiver, RpcRequest} from "./rpc";
import {DistinctStrings} from "./distinctStrings";
import {FullPage} from "./ui/fullPage";
import {EnumIterators, ICancellable, Pair, PartialResult, saveAs} from "./util";
import {BigTableView, TableTargetAPI} from "./tableTarget";
import {MenuItem, SubMenu, TopMenuItem} from "./ui/menu";
import {IHtmlElement, ViewKind} from "./ui/ui";
import {DataLoaded, getDescription} from "./initialObject";
import {HillviewToplevel} from "./toplevel";
import {NotifyDialog} from "./ui/dialog";
import {SchemaReceiver, TableView} from "./dataViews/tableView";
import {IDataView} from "./ui/dataview";
import {HistogramView} from "./dataViews/histogramView";
import {SchemaClassSerialization} from "./schemaClass";
import {Histogram2DView} from "./dataViews/histogram2DView";
import {HeatmapView} from "./dataViews/heatmapView";
import {SchemaView} from "./dataViews/schemaView";
import {SpectrumView} from "./dataViews/spectrumView";
import {HeavyHittersView} from "./dataViews/heavyHittersView";

export interface IViewSerialization {
    viewKind: ViewKind;
    pageId: number;
    sourcePageId: number;
    title: string;
    remoteObjectId: RemoteObjectId;
    rowCount: number;
    schema: SchemaClassSerialization;
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
        let toBring = columns.filter(c => c.kind == "Category")
            .map(c => c.name)
            .filter(c => !this.categoryCache.has(c));
        return new CategoryValuesRequest(this.remoteObjectId, page, columns, toBring);
    }

    /**
     * Check if the selected object can be combined with the specified one,
     * and if so return it.  Otherwise write an error message and return null.
     */
    getSelected(): Pair<BigTableView, number> {
        return { first: this.selected, second: this.selectedPageId }
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
        let combineMenu: MenuItem[] = [];
        combineMenu.push({
            text: "Select current",
            action: () => { this.select(ro, pageId); },
            help: "Save the current view; later it can be combined with another view, using one of the operations below."
        });
        combineMenu.push({text: "---", action: null, help: null});
        EnumIterators.getNamesAndValues(CombineOperators)
            .forEach(c => combineMenu.push({
                text: c.name,
                action: () => { ro.combine(c.value); },
                help: "Combine the rows in the two views using the " + c.value + " operation"
            }));
        return {
            text: "Combine",
            help: "Combine data from two separate views.",
            subMenu: new SubMenu(combineMenu)
        };
    }

    findIndex(page: FullPage): number {
        let index = this.allPages.indexOf(page);
        if (index < 0)
            throw "Page not found";
        return index;
    }

    /**
     * Insert a page after the specified page.
     * @param {FullPage} toInsert  Page to insert.
     * @param {FullPage} after     Page to insert after; if null insertion is done at the end.
     */
    insertAfter(toInsert: FullPage, after: FullPage | null): void {
        console.assert(toInsert != null);
        let pageRepresentation = toInsert.getHTMLRepresentation();
        if (after == null) {
            this.topLevel.appendChild(pageRepresentation);
            this.allPages.push(toInsert);
        } else {
            let index = this.findIndex(after);
            this.allPages.splice(index + 1, 0, toInsert);
            if (index >= this.topLevel.children.length - 1)
                this.topLevel.appendChild(pageRepresentation);
            else
                this.topLevel.insertBefore(pageRepresentation, this.topLevel.children[index + 1]);
        }
    }

    public remove(page: FullPage): void {
        let index = this.findIndex(page);
        this.allPages.splice(index, 1);
        this.topLevel.removeChild(this.topLevel.children[index]);
    }

    public newPage(title: string, sourcePage: FullPage | null): FullPage {
        let number = this.pageCounter++;
        let page = new FullPage(number, title, sourcePage != null ? sourcePage.pageId : null, this);
        this.insertAfter(page, sourcePage);
        return page;
    }

    /**
     * Creates a page when reconstructing a view that has been saved/bookmarked.
     * The newly created page is always inserted at the end.
     */
    public reconstructPage(title: string, pageNo: number, sourcePageNo: number | null): FullPage {
        let page = new FullPage(pageNo, title, sourcePageNo, this);
        if (pageNo >= this.pageCounter)
            this.pageCounter = pageNo + 1;
        this.insertAfter(page, null);
        return page;
    }

    public scrollIntoView(pageId: number): boolean {
        for (let p of this.allPages) {
            if (p.pageId == pageId) {
                p.scrollIntoView();
                return true;
            }
        }
        return false;
    }

    public resize(): void {
        for (let p of this.allPages)
            p.onResize();
    }

    /**
     * Reconstruct one view in the dataset.
     * @param {Object} obj  Object which is a serialization of a BigTableView.
     * @returns {boolean}   True if reconstruction succeeds.
     */
    reconstructView(obj: Object): boolean {
        // This is ugly, but circular module dependences make it
        // difficult to place this method in a set of separate classes.
        let vs = <IViewSerialization>obj;
        if (vs.pageId == null ||
            vs.remoteObjectId == null ||
            vs.rowCount == null ||
            vs.title == null ||
            vs.viewKind == null)  // sourcePageId can be null
            return false;
        let page = this.reconstructPage(vs.title, vs.pageId, vs.sourcePageId);
        let view: IDataView = null;
        switch (vs.viewKind) {
            case "Table":
                view = TableView.reconstruct(vs, page);
                break;
            case "Histogram":
                view = HistogramView.reconstruct(vs, page);
                break;
            case "2DHistogram":
                view = Histogram2DView.reconstruct(vs, page);
                break;
            case "Heatmap":
                view = HeatmapView.reconstruct(vs, page);
                break;
            case "Schema":
                view = SchemaView.reconstruct(vs, page);
                break;
            case "Trellis":
                // TODO
                break;
            case "HeavyHitters":
                view = HeavyHittersView.reconstruct(vs, page);
                break;
            case "SVD Spectrum":
                view = SpectrumView.reconstruct(vs, page);
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
    reconstruct(obj: Object): boolean {
        if (obj["views"] == null)
            return false;
        if (!Array.isArray(obj["views"]))
            return false;
        for (let v of obj["views"])
            if (!this.reconstructView(v))
                return false;
        return true;
    }

    serialize(): IDatasetSerialization {
        let result: IDatasetSerialization = {
            remoteObjectId: this.remoteObjectId,
            views: [],
            kind: "Saved dataset"
        };
        for (let p of this.allPages) {
            let vs = <BigTableView>p.getDataView();
            if (vs != null)
                result.views.push(vs.serialize());
        }
        return result;
    }

    /**
     * Displays again the original data.
     */
    redisplay(): void {
        let rr = this.remoteObject.createGetSchemaRequest();
        let title = getDescription(this.loaded);
        let newPage = this.newPage(title, null);
        rr.invoke(new SchemaReceiver(newPage, rr, this.remoteObject, this, false));
    }

    saveToFile(): void {
        let ser = this.serialize();
        let str = JSON.stringify(ser);
        let fileName = "savedView.txt";
        saveAs(fileName, str);
        let notify = new NotifyDialog("File has been saved\n" +
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

    run(value: IDistinctStrings[]): void {
        // Receive the columns that we asked for and cache the results
        console.assert(this.toBringColumns.length == value.length);
        for (let i = 0; i < this.toBringColumns.length; i++) {
            let ds = new DistinctStrings(value[i], this.toBringColumns[i]);
            if (ds.truncated)
                this.page.reportError(
                    "Column " + this.requestedColumns[i] + " has too many distinct values for a category");
            this.page.dataset.setDistinctStrings(this.toBringColumns[i], ds);
        }

        // Build the result expected by the observer: all the requested columns
        let result: DistinctStrings[] = [];
        for (let c of this.requestedColumns)
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
        let cvo = new CategoryValuesObserver(
            this.page, this, this.requestedColumns, this.toBringColumns, onReply);
        if (this.toBringColumns.length == 0) {
            cvo.onNext(new PartialResult<DistinctStrings[]>(1, []));
            cvo.onCompleted();
        } else {
            super.invoke(cvo);
        }
    }
}
