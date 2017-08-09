/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {IHtmlElement} from "./ui";
export interface MenuItem {
    text: string;
    action: () => void;
}

export class ContextMenu implements IHtmlElement {
    items: MenuItem[];
    private outer: HTMLElement;
    private htmlTable: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis: MenuItem[]) {
        this.outer = document.createElement("div");
        this.outer.className = "dropdown";
        this.outer.onmouseleave = () => this.remove();
        this.htmlTable = document.createElement("table");
        this.outer.appendChild(this.htmlTable);
        this.tableBody = this.htmlTable.createTBody();

        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    public remove(): void {
        this.getHTMLRepresentation().remove();
    }

    addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        cell.innerHTML = mi.text;
        cell.style.textAlign = "left";
        cell.className = "menuItem";
        cell.onclick = () => { this.remove(); mi.action(); }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}


export class TopSubMenu implements IHtmlElement {
    items: MenuItem[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis: MenuItem[]) {
        this.outer = document.createElement("table");
        this.outer.className = "menu";
        this.tableBody = this.outer.createTBody();
        
        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        cell.innerHTML = mi.text;
        cell.style.textAlign = "left";
        cell.className = "menuItem";
        cell.onclick = () => { mi.action(); }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}

export interface TopMenuItem {
    readonly text: string;
    readonly subMenu: TopSubMenu;
}

export class TopMenu implements IHtmlElement {
    items: TopMenuItem[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis: TopMenuItem[]) {
        this.outer = document.createElement("table");
        this.outer.className = "menu";
        this.tableBody = this.outer.createTBody();
        this.tableBody.insertRow();
        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    addItem(mi: TopMenuItem): void {
        let cell = this.tableBody.rows.item(0).insertCell();
        cell.textContent = mi.text;
        cell.appendChild(mi.subMenu.getHTMLRepresentation());
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}