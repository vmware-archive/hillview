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

abstract class BaseMenu implements IHtmlElement {
    items: MenuItem[];
    /**
     * Actual html representations corresponding to the submenu items.
     * They are stored in the same order as the items.
     */
    outer: HTMLTableElement;
    tableBody: HTMLTableSectionElement;
    cells: HTMLTableDataCellElement[];
    selectedIndex: number;  // -1 if no item is selected

    constructor(mis: MenuItem[]) {
        this.items = [];
        this.cells = [];
        this.selectedIndex = -1;
        this.outer = document.createElement("table");
        this.outer.classList.add("menu", "hidden");
        this.tableBody = this.outer.createTBody();
        this.outer.onkeydown = e => this.keyAction(e);
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi, true);
        }
    }

    abstract setAction(mi: MenuItem, enabled: boolean): void;

    keyAction(e: KeyboardEvent): void {
        if (e.code == "ArrowDown" && this.selectedIndex < this.cells.length - 1) {
            this.select(this.selectedIndex + 1);
        } else if (e.code == "ArrowUp"  && this.selectedIndex > 0) {
            this.select(this.selectedIndex - 1);
        } else if (e.code == "Enter" && this.selectedIndex >= 0) {
            // emulate a mouse click on this cell
            this.cells[this.selectedIndex].click();
        } else if (e.code == "Escape") {
            this.hide();
        }
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

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    hide(): void {
        this.outer.classList.add("hidden");
    }

    getCell(mi: MenuItem): HTMLTableCellElement {
        let index = this.find(mi.text);
        if (index < 0)
            throw "Cannot find menu item";
        return this.cells[index];
    }

    public addItem(mi: MenuItem, enabled: boolean): HTMLTableDataCellElement {
        let index = this.items.length;
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
        cell.onmouseenter = () => this.select(index);
        cell.onmouseleave = () => this.select(-1);
        this.enable(mi.text, enabled);
        return cell;
    }

    /**
     * Highlight a menu item (based on mouse position on keyboard actions).
     * @param {number} index      Index of item to highlight.
     * @param {boolean} selected  True if the item is being selected.
     */
    markSelect(index: number, selected: boolean): void {
        if (index >= 0 && index < this.cells.length) {
            let cell = this.cells[index];
            if (selected) {
                cell.classList.add("selected");
                this.outer.focus();
            } else {
                cell.classList.remove("selected");
            }
        }
    }

    select(index: number): void {
        if (this.selectedIndex >= 0)
            this.markSelect(this.selectedIndex, false);
        if (index < 0 || index >= this.cells.length)
            index = -1;  // no one
        this.selectedIndex = index;
        this.markSelect(this.selectedIndex, true);
    }

    enableByIndex(index: number, enabled: boolean): void {
        let cell = this.cells[index];
        if (enabled)
            cell.classList.remove("disabled");
        else
            cell.classList.add("disabled");
        this.setAction(this.items[index], enabled);
    }

    enableItem(mi: MenuItem, enabled: boolean): void {
        this.enable(mi.text, enabled);
    }

    /**
     * Remove all elements from the menu.
     */
    clear(): void {
        this.tableBody.remove();
        this.items = [];
        this.cells = [];
        this.tableBody = this.outer.createTBody();
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
        this.enableByIndex(index, enabled);
    }
}

/**
 * A context menu is displayed on right-click on some displayed element.
 */
export class ContextMenu extends BaseMenu implements IHtmlElement {
    /**
     * Create a context menu.
     * @param parent            HTML element where this is inserted.
     * @param {MenuItem[]} mis  List of menu items in the context menu.
     */
    constructor(parent: HTMLElement, mis?: MenuItem[]) {
        super(mis);
        this.outer.classList.add("dropdown");
        this.outer.classList.add("menu");
        this.outer.onmouseleave = () => { this.hide() };

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
        this.outer.classList.remove("hidden");
        this.outer.tabIndex = 1;  // necessary for keyboard events?
        this.outer.focus();
    }

    /**
     * Place the menu at some specific position on the screen.
     * @param {number} x  Absolute x coordinate.
     * @param {number} y  Absolute y coordinate.
     */
    move(x: number, y: number): void {
        this.outer.style.transform = `translate(${x}px, ${y}px)`;
    }

    setAction(mi: MenuItem, enabled: boolean): void {
        let index = this.find(mi.text);
        let cell = this.cells[index];
        if (mi.action != null && enabled)
            cell.onclick = () => { this.hide(); mi.action(); };
        else
            cell.onclick = () => this.hide();
    }
}

/**
 * A sub-menu is a menu displayed when selecting an item
 * from a topmenu.
 */
export class SubMenu extends BaseMenu implements IHtmlElement {
    /**
     * Build a submenu.
     * @param {MenuItem[]} mis  List of items to display.
     */
    constructor(mis: MenuItem[]) {
        super(mis);
        this.outer.id = "topMenu";
    }

    show(): void {
        this.outer.classList.remove("hidden");
        this.outer.tabIndex = 1;  // necessary for keyboard events?
    }

    setAction(mi: MenuItem, enabled: boolean): void {
        let cell = this.getCell(mi);
        if (mi.action != null && enabled)
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); mi.action(); };
        else
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); }
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
