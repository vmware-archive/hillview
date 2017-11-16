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

import {IHtmlElement, removeAllChildren} from "./ui";
import {FullPage} from "./fullPage";

/**
 * Main interface implemented by all Hillview views.
 * Any IDataView resides within a page.
 */
export interface IDataView extends IHtmlElement {
    /**
     * Set the page where the dataview is displayed.
     */
    setPage(page: FullPage): void;
    /**
     * Get the page where the dataview is displayed.
     */
    getPage(): FullPage;
    /**
     * This method can be called automatically when the browser window is
     * being resized, or it can be called as a result of a user action.
     */
    refresh(): void;
}

/**
 * A DataDisplay holds an IDataView within a FullPage.
 */
export class DataDisplay implements IHtmlElement {
    topLevel: HTMLElement;
    element: IDataView;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "dataDisplay";
    }

    public onResize(): void {
        if (this.element != null)
            this.element.refresh();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public setDataView(element: IDataView): void {
        this.element = element;
        removeAllChildren(this.topLevel);
        this.topLevel.appendChild(element.getHTMLRepresentation());
    }
}
