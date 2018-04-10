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
    protected datasetCounter: number;
    protected current: Dataset;

    public static readonly instance = new HillviewToplevel();

    private constructor() {
        this.datasets = [];
        this.datasetCounter = 0;
        this.current = null;
        this.topLevel = document.createElement("div");
        let page = new FullPage(0, "Load data", null, null);

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
        // tab names never change once tabs are created.  They are also unique.
        // The tab name is not the dataset name; the dataset name is displayed in the tab.
        let tabName = "tab" + this.datasetCounter++;
        this.datasets.push(dataset);

        let tab = document.createElement("table");
        tab.className = "tab";
        tab.id = tabName;
        let row = tab.insertRow();

        this.strip.appendChild(tab);
        this.tabs.push(tab);
        let cell = row.insertCell();
        cell.textContent = dataset.name;
        cell.title = dataset.name;
        cell.onclick = () => { if (this.select(tabName)) this.rename(tabName); };
        cell.className = "dataset-name";

        let close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.setAttribute("float", "right");
        close.onclick = () => this.remove(tabName);
        close.title = "Close this dataset.";
        cell = row.insertCell();
        cell.appendChild(close);
        this.select(tabName);
    }

    index(tabName: string): number {
        let index = this.tabs.map(d => d.id).lastIndexOf(tabName);
        console.assert(index >= 0);
        return index;
    }

    rename(tabName: string): void {
        let index = this.index(tabName);
        let cell = <HTMLElement>this.tabs[index].querySelector(".dataset-name");
        cell.onclick = null;
        let oldName = cell.textContent;
        let input = document.createElement("input");
        cell.textContent = "";
        cell.insertBefore(input, cell.children[0]);
        input.value = oldName;
        input.type = "text";
        input.onblur = input.onchange = () => this.renamed(cell, input, tabName);
    }

    renamed(cell: HTMLElement, input: HTMLInputElement, tabName: string): void {
        let newName = input.value;
        let index = this.index(tabName);
        this.datasets[index].rename(newName);
        cell.textContent = newName;
        cell.onclick = () => { if (this.select(tabName)) this.rename(tabName); };
    }

    remove(tabName: string): void {
        let index = this.index(tabName);
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
    select(tabName: string): boolean {
        let index = this.index(tabName);
        let selected: boolean = false;
        for (let i = 0; i < this.strip.childElementCount; i++) {
            let child: HTMLElement = this.tabs[i];
            if (i != index) {
                child.classList.remove("current");
            } else {
                if (child.classList.contains("current"))
                    selected = true;
                child.classList.add("current");
            }
        }

        removeAllChildren(this.content);
        let dataset = this.datasets[index];
        this.current = dataset;
        this.content.appendChild(dataset.getHTMLRepresentation());
        return selected;
    }

    resize(): void {
        this.current.resize();
    }
}

export function createHillview(): void {
    let top = document.getElementById('top');
    top.appendChild(HillviewToplevel.instance.getHTMLRepresentation());
    window.addEventListener("resize",  () => HillviewToplevel.instance.resize());
}
