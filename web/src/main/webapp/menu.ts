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

import {IHtmlElement} from "./ui";
export interface MenuItem {
    text: string;
    action: () => void;
}

export class ContextMenu implements IHtmlElement {
    items: MenuItem[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis?: MenuItem[]) {
        this.outer = document.createElement("table");
        this.outer.classList.add("dropdown");
        this.outer.classList.add("menu");
        this.outer.onmouseleave = () => {this.hide()};
        this.tableBody = this.outer.createTBody();

        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
        this.hide();
    }

    public show(): void {
        this.outer.hidden = false;
    }

    public hide(): void {
        this.outer.hidden = true;
    }

    public clear(): void {
        this.tableBody.remove();
        this.tableBody = this.outer.createTBody();
    }

    public move(x: number, y: number): void {
        this.outer.style.transform =
            "translate("
                + x + "px , "
                + y + "px"
            + ")";
    }

    public addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        cell.innerHTML = mi.text;
        cell.style.textAlign = "left";
        cell.className = "menuItem";
        if (mi.action != null)
            cell.onclick = () => { this.hide(); mi.action(); };
        else
            cell.onclick = () => this.hide();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}

export class TopSubMenu implements IHtmlElement {
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis: MenuItem[]) {
        this.outer = document.createElement("table");
        this.outer.classList.add("menu", "hidden");
        this.tableBody = this.outer.createTBody();
        this.outer.onmouseleave = () => {console.log("Hiding!"); this.hide()};
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    addItem(mi: MenuItem): void {
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        if (mi.text == "---")
            cell.innerHTML = "<hr>";
        else
            cell.innerHTML = mi.text;
        cell.style.textAlign = "left";
        cell.classList.add("menuItem");
        if (mi.action != null)
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); mi.action(); }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    show(): void {
        this.outer.classList.remove("hidden");
    }

    hide(): void {
        this.outer.classList.add("hidden");
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
        this.outer.classList.add("menu");
        this.outer.classList.add("topMenu");
        this.tableBody = this.outer.createTBody();
        this.tableBody.insertRow();
        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    hideSubMenus(): void {
        this.items.forEach((mi) => mi.subMenu.hide());
    }

    addItem(mi: TopMenuItem): void {
        let cell = this.tableBody.rows.item(0).insertCell();
        cell.textContent = mi.text;
        cell.appendChild(mi.subMenu.getHTMLRepresentation());
        cell.onclick = () => {this.hideSubMenus(); mi.subMenu.show()};
        this.items.push(mi);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}
