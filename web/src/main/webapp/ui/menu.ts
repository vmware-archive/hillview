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
import {makeId} from "../util";

/**
 * One item in a menu.
 */
export interface MenuItem {
    /**
     * Text that is written on screen when menu is displayed.
     */
    readonly text: string;
    /**
     * String that is displayed as a help popup.
     */
    readonly help: string;
    /**
     * Action that is executed when the item is selected by the user.
     */
    readonly action: () => void;
}

/**
 * A context menu is displayed on right-click on some displayed element.
 */
export class ContextMenu implements IHtmlElement {
    items: MenuItem[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    /**
     * Create a context menu.
     * @oaran parent            HTML element where this is inserted.
     * @param {MenuItem[]} mis  List of menu items in the context menu.
     */
    constructor(parent: HTMLElement, mis?: MenuItem[]) {
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
        parent.appendChild(this.getHTMLRepresentation());
        this.hide();
    }

    /**
     * Display the menu.
     */
    public show(e: MouseEvent): void {
        e.preventDefault();
        // Spawn the menu at the mouse's location
        this.move(e.pageX - 1, e.pageY - 1);
        this.outer.hidden = false;
    }

    /**
     * Hide the menu.
     */
    public hide(): void {
        this.outer.hidden = true;
    }

    /**
     * Remove all elements from the menu.
     */
    clear(): void {
        this.tableBody.remove();
        this.tableBody = this.outer.createTBody();
    }

    /**
     * Place the menu at some specific position on the screen.
     * @param {number} x  Absolute x coordinate.
     * @param {number} y  Absolute y coordinate.
     */
    move(x: number, y: number): void {
        this.outer.style.transform = `translate(${x}px, ${y}px)`;
    }

    addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        cell.innerHTML = mi.text;
        if (mi.help != null)
            cell.title = mi.help;
        cell.style.textAlign = "left";
        cell.className = "menuItem";
        cell.id = makeId(mi.text);
        if (mi.action != null)
            cell.onclick = () => { this.hide(); mi.action(); };
        else
            cell.onclick = () => this.hide();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}

/**
 * A sub-menu is a menu displayed when selecting an item
 * from a topmenu.
 */
export class SubMenu implements IHtmlElement {
    /**
     * Menu items describing the submenu.
     */
    private items: MenuItem[];
    /**
     * Actual html representations corresponding to the submenu items.
     * They are stored in the same order as the items.
     */
    private cells: HTMLTableDataCellElement[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    /**
     * Build a submenu.
     * @param {MenuItem[]} mis  List of items to display.
     */
    constructor(mis: MenuItem[]) {
        this.items = [];
        this.cells = [];
        this.outer = document.createElement("table");
        this.outer.classList.add("menu", "hidden");
        this.outer.id = "topMenu";
        this.tableBody = this.outer.createTBody();
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        this.cells.push(cell);
        if (mi.text == "---")
            cell.innerHTML = "<hr>";
        else
            cell.innerHTML = mi.text;
        cell.id = makeId(mi.text);
        cell.style.textAlign = "left";
        if (mi.help != null)
            cell.title = mi.help;
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

    /**
     * Find the position of an item based on its text.
     * @param text   Text string in the item.
     */
    find(text: string): number {
        for (let i=0; i < this.items.length; i++)
            if (this.items[i].text == text)
                return i;
        return -1;
    }

    /**
     * Mark a menu item as enabled or disabled.  If disabled the action
     * cannot be triggered.
     * @param {string} text      Text of the item which identifies the item.
     * @param {boolean} enabled  If true the menu item is enabled, else it is disabled.
     */
    enable(text: string, enabled: boolean): void {
        let index = this.find(text);
        if (index < 0)
            throw "Cannot find menu item " + text;
        let cell = this.cells[index];
        if (enabled) {
            let mi = this.items[index];
            cell.classList.remove("disabled");
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); mi.action(); }
        } else {
            cell.classList.add("disabled");
            cell.onclick = null;
        }
    }
}

/**
 * A topmenu is composed only of submenus.
 * TODO: allow a topmenu to also have simple items.
 */
export interface TopMenuItem {
    readonly text: string;
    readonly subMenu: SubMenu;
    readonly help: string;
}

/**
 * A TopMenu is a two-level menu.
 */
export class TopMenu implements IHtmlElement {
    items: TopMenuItem[];
    private outer: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    /**
     * Create a topmenu.
     * @param {TopMenuItem[]} mis  List of top menu items to display.
     */
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
        cell.id = makeId(mi.text);  // for testing
        cell.textContent = mi.text;
        cell.appendChild(mi.subMenu.getHTMLRepresentation());
        cell.onclick = () => {this.hideSubMenus(); mi.subMenu.show()};
        cell.onmouseleave = () => this.hideSubMenus();
        if (mi.help != null)
            cell.title = mi.help;
        this.items.push(mi);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    /**
     * Find the position of an item
     * @param text   Text string in the item.
     */
    getSubmenu(text: string): SubMenu {
        for (let i=0; i < this.items.length; i++)
            if (this.items[i].text == text)
                return this.items[i].subMenu;
        return null;
    }
}
