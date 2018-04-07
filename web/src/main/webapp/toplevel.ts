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

import {IHtmlElement, removeAllChildren} from "./ui/ui";
import {LoadMenu} from "./loadMenu";
import {InitialObject} from "./initialObject";
import {FullPage} from "./ui/fullPage";
import {Dataset} from "./dataset";

/**
 * The toplevel class implements the web page structure for Hillview.
 * The web page has a load menu, a list of tabs, and the view of the current tab.
 * Each tab corresponds to one Dataset.
 */
export class HillviewToplevel implements IHtmlElement {
    private readonly topLevel: HTMLElement;
    private readonly datasets: Dataset[];
    private readonly strip: HTMLDivElement;
    private readonly tabs: HTMLElement[];
    private readonly content: HTMLDivElement;

    public static readonly instance = new HillviewToplevel();

    private constructor() {
        this.datasets = [];
        this.topLevel = document.createElement("div");
        let page = new FullPage(0, null, null, null);

        this.topLevel.appendChild(page.getHTMLRepresentation());
        let menu = new LoadMenu(InitialObject.instance, page);
        page.setDataView(menu);
        page.getTitleElement().onclick = () => menu.toggleAdvanced();

        let tabStrip = document.createElement("div");
        this.topLevel.appendChild(tabStrip);
        this.strip = document.createElement("div");
        this.strip.className = "tabs-strip";
        this.strip.style.display = "flex";
        this.strip.style.width = "100%";
        this.strip.style.flexDirection = "row";
        this.strip.style.flexWrap = "nowrap";
        this.strip.style.alignItems = "center";

        tabStrip.appendChild(this.strip);
        this.tabs = [];

        this.content = document.createElement("div");
        this.topLevel.appendChild(this.content);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public addDataset(dataset: Dataset): void {
        this.datasets.push(dataset);

        let tab = document.createElement("table");
        tab.className = "tab";
        let row = tab.insertRow();

        this.strip.appendChild(tab);
        this.tabs.push(tab);
        let cell = row.insertCell();
        cell.innerText = dataset.name;
        cell.title = dataset.name;
        cell.onclick = () => this.select(dataset.name);

        let close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.setAttribute("float", "right");
        close.onclick = () => this.remove(dataset.name);
        close.title = "Close this dataset.";
        cell = row.insertCell();
        cell.appendChild(close);
        this.select(dataset.name);
    }

    remove(name: string): void {
        let index = this.datasets.map(d => d.name).lastIndexOf(name);
        this.strip.removeChild(this.tabs[index]);
        this.datasets.splice(index, 1);
        this.tabs.splice(index, 1);
        if (this.tabs.length >= 1)
            this.select(this.datasets[0].name);
    }

    select(name: string): void {
        let index = this.datasets.map(d => d.name).lastIndexOf(name);
        for (let i = 0; i < this.strip.childElementCount; i++) {
            let child: HTMLElement = this.tabs[i];
            if (i != index)
                child.classList.remove("current");
            else
                child.classList.add("current");
        }

        removeAllChildren(this.content);
        let dataset = this.datasets[index];
        this.content.appendChild(dataset.getHTMLRepresentation());
    }

    resize(): void {
        // Called when the view is resized.
        // TODO.
    }
}

export function createHillview(): void {
    let top = document.getElementById('top');
    top.appendChild(HillviewToplevel.instance.getHTMLRepresentation());
    window.addEventListener("resize",  () => HillviewToplevel.instance.resize());
}
