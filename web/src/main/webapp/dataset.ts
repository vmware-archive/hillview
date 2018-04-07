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

import {
    CombineOperators,
    RemoteObjectId
} from "./javaBridge";
import {OnCompleteRenderer, RemoteObject, RpcRequest} from "./rpc";
import {DistinctStrings, IDistinctStrings} from "./distinctStrings";
import {FullPage} from "./ui/fullPage";
import {EnumIterators, ICancellable, Pair, PartialResult} from "./util";
import {RemoteTableObjectView} from "./tableTarget";
import {MenuItem, SubMenu, TopMenuItem} from "./ui/menu";
import {IHtmlElement} from "./ui/ui";
import {DataLoaded} from "./initialObject";
import {HillviewToplevel} from "./toplevel";


/**
 * A dataset holds all information related to a loaded dataset.
 * A dataset represents the original (remote) data loaded from some storage medium.
 * A dataset will then have many views.
 */
export class Dataset implements IHtmlElement {
    public readonly remoteObject: RemoteObject;
    private categoryCache: Map<string, DistinctStrings>;
    private selected: RemoteTableObjectView; // participates in a combine operation
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
                public readonly name: string,
                public readonly loaded: DataLoaded) {
        this.remoteObject = new RemoteObject(remoteObjectId);
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

    public select(object: RemoteTableObjectView, pageId: number): void {
        this.selected = object;
        this.selectedPageId = pageId;
    }

    public toString(): string {
        return this.remoteObjectId;
    }

    /// Retrieves the category values for all specified column names
    /// and stores them internally in columnValues.
    /// Invokes continuation when all values are known.
    public retrieveCategoryValues(columnNames: string[], page: FullPage,
                                  // The operation is the asynchronous operation
                                  // that may have retrieved the data
                                  continuation: (operation: ICancellable) => void): void {
        let columnsToFetch: string[] = [];
        for (let c of columnNames) {
            if (!this.categoryCache.has(c))
                columnsToFetch.push(c);
        }

        let rr = this.remoteObject.createStreamingRpcRequest<IDistinctStrings[]>("uniqueStrings", columnsToFetch);
        if (columnsToFetch.length > 0) {
            let renderer = new ReceiveCategory(this, columnsToFetch, continuation, page, rr);
            rr.invoke(renderer);
        } else {
            continuation(rr);
        }
    }

    /**
     * Check if the selected object can be combined with the specified one,
     * and if so return it.  Otherwise write an error message and return null.
     */
    getSelected(): Pair<RemoteTableObjectView, number> {
        return { first: this.selected, second: this.selectedPageId }
    }

    public setDistinctStrings(columnName: string, values: DistinctStrings): void {
        this.categoryCache.set(columnName, values);
    }

    public getDistinctStrings(columnName: string): DistinctStrings {
        return this.categoryCache.get(columnName);
    }

    public combineMenu(ro: RemoteTableObjectView, pageId: number): TopMenuItem {
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

    public append(page: FullPage): void {
        this.allPages.push(page);
        this.topLevel.appendChild(page.getHTMLRepresentation());
    }

    public insertAfterMe(toInsert: FullPage, me: FullPage): void {
        console.assert(toInsert != null);
        let pageRepresentation = toInsert.getHTMLRepresentation();
        if (me == null) {
            this.topLevel.appendChild(pageRepresentation);
            this.allPages.push(toInsert);
        } else {
            let index = this.findIndex(me);
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

    public newPage(title: string, sourcePage: FullPage): FullPage {
        let number = this.pageCounter++;
        let page = new FullPage(number, title, sourcePage, this);
        this.insertAfterMe(page, sourcePage);
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
}

/**
 * Receives a list of DistinctStrings and stores them into the category cache.
 * After that it calls the supplied continuation.
 */
class ReceiveCategory extends OnCompleteRenderer<IDistinctStrings[]> {
    public constructor(
        protected dataset: Dataset,
        protected columns: string[],
        protected continuation: (operation: ICancellable) => void,
        page: FullPage,
        operation: RpcRequest<PartialResult<IDistinctStrings[]>>) {
        super(page, operation, "Create converter");
    }

    public run(value: IDistinctStrings[]): void {
        if (this.columns.length != value.length)
            throw "Required " + this.columns.length + " got " + value.length;
        for (let i=0; i < value.length; i++) {
            let col = this.columns[i];
            if (value[i].truncated) {
                this.page.reportError("Column " + col + " has too many distinct values; it is not really a category");
            } else {
                let ds = new DistinctStrings(value[i]);
                this.dataset.setDistinctStrings(col, ds);
            }
        }
        this.continuation(this.operation);
    }
}
