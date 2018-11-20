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

import {DatasetView} from "./datasetView";
import {InitialObject} from "./initialObject";
import {LoadMenu} from "./loadMenu";
import {FullPage, PageTitle} from "./ui/fullPage";
import {ContextMenu} from "./ui/menu";
import {HtmlString, IHtmlElement, removeAllChildren} from "./ui/ui";
import {assert} from "./util";

/**
 * The toplevel class implements the web page structure for Hillview.
 * The web page has a load menu, a list of tabs, and the view of the current tab.
 * Each tab corresponds to one Dataset.
 */
export class HillviewToplevel implements IHtmlElement {
    private readonly topLevel: HTMLElement;
    private readonly datasets: DatasetView[];
    private readonly strip: HTMLDivElement;
    private readonly tabs: HTMLElement[];
    private readonly content: HTMLDivElement;
    protected datasetCounter: number;
    protected current: DatasetView;

    public static readonly instance = new HillviewToplevel();

    private constructor() {
        this.datasets = [];
        this.datasetCounter = 0;
        this.current = null;
        this.topLevel = document.createElement("div");
        const page = new FullPage(0, new PageTitle("Load data"), null, null);

        this.topLevel.appendChild(page.getHTMLRepresentation());
        const menu = new LoadMenu(InitialObject.instance, page);
        page.setDataView(menu);
        page.getTitleElement().ondblclick = () => menu.toggleAdvanced();

        const tabStrip = document.createElement("div");
        this.topLevel.appendChild(document.createElement("hr"));
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

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public addDataset(dataset: DatasetView): void {
        // tab names never change once tabs are created.  They are also unique.
        // The tab name is not the dataset name; the dataset name is displayed in the tab.
        const tabName = "tab" + this.datasetCounter++;
        this.datasets.push(dataset);

        const tab = document.createElement("table");
        tab.className = "tab";
        tab.id = tabName;
        const row = tab.insertRow();

        this.strip.appendChild(tab);
        this.tabs.push(tab);
        let cell = row.insertCell();
        cell.textContent = dataset.name;
        cell.title = dataset.name;
        cell.onclick = () => { if (this.select(tabName)) { this.rename(tabName); } };
        cell.className = "dataset-name";

        const menu = new ContextMenu(this.topLevel, [{
            text: "Save to file",
            action: () => dataset.saveToFile(),
            help: "Save data describing this view to a local file;\n" +
            "this file can be shared with other people.",
        }, {
            text: "Redisplay",
            action: () => dataset.redisplay(),
            help: "Display again the original dataset",
        }]);
        cell.oncontextmenu = (e) => menu.show(e);

        const close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.setAttribute("float", "right");
        close.onclick = () => this.remove(tabName);
        close.title = "Close this dataset.";
        cell = row.insertCell();
        cell.appendChild(close);
        this.select(tabName);
    }

    public index(tabName: string): number {
        const index = this.tabs.map((d) => d.id).lastIndexOf(tabName);
        assert(index >= 0);
        return index;
    }

    public rename(tabName: string): void {
        const index = this.index(tabName);
        const cell = this.tabs[index].querySelector(".dataset-name") as HTMLElement;
        cell.onclick = null;
        const oldName = cell.textContent;
        const input = document.createElement("input");
        cell.textContent = "";
        cell.insertBefore(input, cell.children[0]);
        input.value = oldName;
        input.type = "text";
        input.onmouseout = input.onchange = () => this.renamed(cell, input, tabName);
    }

    public renamed(cell: HTMLElement, input: HTMLInputElement, tabName: string): void {
        const newName = input.value;
        const index = this.index(tabName);
        this.datasets[index].rename(newName);
        cell.textContent = newName;
        cell.onclick = () => { if (this.select(tabName)) { this.rename(tabName); } };
    }

    public remove(tabName: string): void {
        const index = this.index(tabName);
        this.strip.removeChild(this.tabs[index]);
        this.datasets.splice(index, 1);
        this.tabs.splice(index, 1);
        if (this.tabs.length >= 1) {
            this.select(this.tabs[0].id);
        } else {
            removeAllChildren(this.content);
            this.current = null;
        }
    }

    /**
     * Select the tab with the specified name (a string like tabXXX).
     * @param tabName    Name of tab to select.
     * @returns True if the same tab was already selected.
     */
    public select(tabName: string): boolean {
        const index = this.index(tabName);
        let selected: boolean = false;
        for (let i = 0; i < this.strip.childElementCount; i++) {
            const child: HTMLElement = this.tabs[i];
            if (i !== index) {
                child.classList.remove("current");
            } else {
                if (child.classList.contains("current")) {
                    selected = true;
                }
                child.classList.add("current");
            }
        }

        removeAllChildren(this.content);
        const dataset = this.datasets[index];
        this.current = dataset;
        this.content.appendChild(dataset.getHTMLRepresentation());
        dataset.resize();
        return selected;
    }

    public resize(): void {
        if (this.current != null) {
            this.current.resize();
        }
    }
}

export function createHillview(): void {
    const top = document.getElementById("top");
    top.appendChild(HillviewToplevel.instance.getHTMLRepresentation());
    window.addEventListener("resize",  () => HillviewToplevel.instance.resize());
}
