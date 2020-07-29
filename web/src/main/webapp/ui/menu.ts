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

import {browserWindowSize, makeId, px} from "../util";
import {IHtmlElement} from "./ui";

/**
 * One item in a menu.
 */
export interface BaseMenuItem {
    /**
     * Text that is written on screen when menu is displayed.
     */
    readonly text: string;
    /**
     * String that is displayed as a help popup.
     */
    readonly help: string;

}

export interface MenuItem extends BaseMenuItem {
    /**
     * Action that is executed when the item is selected by the user.
     */
    readonly action: () => void;
}

interface subMenuCells {
    cells: HTMLTableDataCellElement[];
    upperBorder: HTMLTableDataCellElement;
    lowerBorder: HTMLTableDataCellElement;
}

function emptySubMenu(): subMenuCells {
    return {
        cells: [],
        upperBorder: undefined,
        lowerBorder: undefined
    }
}

abstract class BaseMenu<MI extends BaseMenuItem> implements IHtmlElement {
    public items: MI[];
    /**
     * Actual html representations corresponding to the submenu items.
     * They are stored in the same order as the items.
     */
    public outer: HTMLTableElement;
    public tableBody: HTMLTableSectionElement;
    public cells: HTMLTableDataCellElement[];
    public subMenuCells: subMenuCells[];
    public rows: HTMLTableRowElement[];
    public selectedIndex: number; // -1 if no item is selected
    public selectedParentMenu: number; // -1 if no item is selected
    public selectedSubMenu: number; // -1 if no item is selected

    protected constructor() {
        this.items = [];
        this.cells = [];
        this.subMenuCells = [];
        this.selectedParentMenu = -1;
        this.rows = [];
        this.selectedIndex = -1;
        this.outer = document.createElement("table");
        this.outer.classList.add("menu", "hidden");
        this.tableBody = this.outer.createTBody();
        this.outer.onkeydown = (e) => this.keyAction(e);
    }

    protected addItems(mis: MI[]): void {
        if (mis != null) {
            for (const mi of mis)
                this.addItem(mi, true);
        }
    }

    public abstract setAction(mi: MI, enabled: boolean): void;

    public keyAction(e: KeyboardEvent): void {
        if (e.code === "ArrowDown" && this.selectedIndex < this.cells.length - 1) {
            this.select(this.selectedIndex + 1);
        } else if (e.code === "ArrowUp" && this.selectedIndex > 0) {
            this.select(this.selectedIndex - 1);
        } else if (e.code === "Enter" && this.selectedIndex >= 0) {
            // emulate a mouse click on this cell
            this.cells[this.selectedIndex].click();
        } else if (e.code === "Escape") {
            this.hide();
        }
    }

    /**
     * Find the position of an item based on its text.
     * @param text   Text string in the item.
     */
    public find(text: string): number {
        for (let i = 0; i < this.items.length; i++)
            if (this.items[i].text === text)
                return i;
        return -1;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    public hide(): void {
        this.outer.classList.add("hidden");
    }

    public getCell(mi: MI): HTMLTableCellElement {
        const index = this.find(mi.text);
        if (index < 0)
            throw new Error("Cannot find menu item");
        return this.cells[index];
    }

    public expandMenu(parentMenu: string) {
        const index = this.find(parentMenu);
        this.selectedParentMenu = index;
        if (index < 0) throw new Error("Cannot find menu item " + parentMenu);
        this.subMenuCells[index].cells.forEach((cell) => {
            cell.style.display = "table-cell";
        });
    }

    public addItem(mi: MI, enabled: boolean): HTMLTableDataCellElement {
        const index = this.items.length;
        this.items.push(mi);
        const trow = this.tableBody.insertRow();
        this.rows.push(trow);
        const cell = trow.insertCell(0);
        this.cells.push(cell);
        if (mi.text === "---")
            cell.innerHTML = "<hr>";
        else
            cell.textContent = mi.text;
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
    public markSelect(index: number, selected: boolean): void {
        if (index >= 0 && index < this.cells.length) {
            const cell = this.cells[index];
            if (selected) {
                // remove submenu if the parentMenu is not selected
                if (this.selectedParentMenu != -1 && index != this.selectedParentMenu)
                    this.hideAllSubMenu();
                // Show subMenu when parent menu is active
                if (cell.classList.contains("expandableMenu"))
                    this.expandMenu(cell.firstChild.textContent);
                cell.classList.add("selected");
                this.outer.focus();
            } else {
                cell.classList.remove("selected");
            }
        }
    }

    public select(index: number): void {
        if (this.selectedIndex >= 0)
            this.markSelect(this.selectedIndex, false);
        if (index < 0 || index >= this.cells.length)
            index = -1;  // no one
        this.selectedIndex = index;
        this.markSelect(this.selectedIndex, true);
    }

    public enableByIndex(index: number, enabled: boolean): void {
        const cell = this.cells[index];
        if (enabled)
            cell.classList.remove("disabled");
        else
            cell.classList.add("disabled");
        this.setAction(this.items[index], enabled);
    }

    public hideAllSubMenu(): void {
        this.subMenuCells[this.selectedParentMenu].cells.forEach((cell) => {
            cell.style.display = "none";
            cell.classList.remove("selected");
        });
        this.markSelect(this.selectedParentMenu, false);
        this.selectedParentMenu = -1;
        this.selectedSubMenu = -1;
    }

    public markSelectSubMenu(index: number, selected: boolean): void {
        if (index >= 0 &&
            index < this.subMenuCells[this.selectedParentMenu].cells.length) {
            const cell = this.subMenuCells[this.selectedParentMenu].cells[index];
            if (selected) {
                cell.classList.add("selected");
                this.outer.focus();
            } else {
                cell.classList.remove("selected");
            }
        }
    }

    public selectSubMenu(parentIndex: number, subIndex: number): void {
        if (subIndex == -1) {
            this.markSelectSubMenu(this.selectedSubMenu, false);
        } else {
            this.markSelect(parentIndex, true);
            if (subIndex < 0 ||
                subIndex >= this.subMenuCells[parentIndex].cells.length) {
                subIndex = -1; // no one
            }
            this.selectedSubMenu = subIndex;
            this.markSelectSubMenu(this.selectedSubMenu, true);
        }
    }

    public enableItem(mi: MI, enabled: boolean): void {
        this.enable(mi.text, enabled);
    }

    /**
     * Remove all elements from the menu.
     */
    public clear(): void {
        this.tableBody.remove();
        this.items = [];
        this.cells = [];
        this.rows = [];
        this.subMenuCells = [];
        this.tableBody = this.outer.createTBody();
    }

    /**
     * Mark a menu item as enabled or disabled.  If disabled the action
     * cannot be triggered.
     * @param {string} text      Text of the item which identifies the item.
     * @param {boolean} enabled  If true the menu item is enabled, else it is disabled.
     */
    public enable(text: string, enabled: boolean): void {
        const index = this.find(text);
        if (index < 0)
            throw new Error("Cannot find menu item " + text);
        this.enableByIndex(index, enabled);
    }
}

/**
 * A context menu is displayed on right-click on some displayed element.
 */
export class ContextMenu extends BaseMenu<MenuItem> implements IHtmlElement {
    /**
     * Create a context menu.
     * @param parent            HTML element where this is inserted.
     * @param {MenuItem[]} mis  List of menu items in the context menu.
     */
    constructor(parent: Element, mis?: MenuItem[]) {
        super();
        this.addItems(mis);
        this.outer.classList.add("dropdown");
        this.outer.classList.add("menu");
        this.outer.onmouseleave = () => { this.hide(); };

        parent.appendChild(this.getHTMLRepresentation());
        this.hide();
    }

    /**
     * Display the menu.
     */
    public show(e: MouseEvent): void {
        e.preventDefault();
        // Spawn the menu at the mouse's location
        let x = e.clientX - 5;
        let y = e.clientY - 5;
        this.outer.classList.remove("hidden");
        const max = browserWindowSize();

        // We use 5 to leave room for border and shadow
        if (this.outer.offsetWidth + x >= max.width)
            x = max.width - this.outer.offsetWidth - 5;
        if (this.outer.offsetHeight + y >= max.height)
            y = max.height - this.outer.offsetHeight - 5;
        if (y < 0)
            y = 0;
        this.move(x, y);
        this.outer.tabIndex = 1;  // necessary for keyboard events?
        this.outer.focus();
    }

    /**
     * Place the menu at some specific position on the screen.
     * @param {number} x  Absolute x coordinate within browser window.
     * @param {number} y  Absolute y coordinate within browser window.
     */
    public move(x: number, y: number): void {
        this.outer.style.left = px(x);
        this.outer.style.top = px(y);
    }

    public setAction(mi: MenuItem, enabled: boolean): void {
        const index = this.find(mi.text);
        const cell = this.cells[index];
        if (mi.action != null && enabled) {
            if (cell.classList.contains("expandableMenu")) {
              // no operation because expandableMenu only expand its submenus
            } else {
              cell.onclick = () => {
                this.hide();
                mi.action();
              };
            }
        } else {
            cell.onclick = () => this.hide();
        }
    }

    public addExpandableItem(mi: MenuItem): number {
        const cell = this.addItem(mi, true);
        cell.classList.add("expandableMenu");
        const arrow = document.createElement("span");
        arrow.textContent = "▸";
        arrow.classList.add("menuArrow");
        cell.appendChild(arrow);
        return this.cells.length - 1;
    }

    /**
     * Inserting subMenu must be done in the end of all parent menu insert. If the
     * submenu exceed the length of all main menu, we need to add dummyMenu
     * */
    public insertSubMenu(parentIndex: number, mi: MenuItem, enabled: boolean,
            isLastSubMenu: boolean): void {
        if (this.subMenuCells[parentIndex] === undefined)
            this.subMenuCells[parentIndex] = emptySubMenu();

        const subMenuIndex = this.subMenuCells[parentIndex].cells.length;
        const subMenuPlacementIdx = parentIndex + subMenuIndex;
        if (this.rows[subMenuPlacementIdx] === undefined) {
            const trow = this.tableBody.insertRow();
            this.rows.push(trow);
            const cell = trow.insertCell(0);
            cell.classList.add("dummyMenu");
        }
        const cell = this.rows[subMenuPlacementIdx].insertCell(1);
        cell.id = makeId(mi.text);
        cell.style.textAlign = "left";
        cell.style.display = "none";
        cell.classList.add("menuItem");
        if (enabled) cell.classList.remove("disabled");
        else cell.classList.add("disabled");
        if (mi.help != null) cell.title = mi.help;
        if (mi.text === "---") cell.innerHTML = "<hr>";
        else cell.textContent = mi.text;

        var subIndex = this.subMenuCells[parentIndex].cells.length;
        this.subMenuCells[parentIndex].cells.push(cell);
        cell.onmouseenter = () => this.selectSubMenu(parentIndex, subIndex);
        cell.onmouseleave = () => this.selectSubMenu(parentIndex, -1);
        this.setSubMenuAction(parentIndex, subMenuIndex, mi, enabled);

        // Adding upperBorder: to clear the menu when the cursor hover above 1st submenu
        if (subMenuPlacementIdx > 0 && this.subMenuCells[parentIndex].cells.length == 1) {
            const upperBorder = this.rows[subMenuPlacementIdx - 1].insertCell(1);
            upperBorder.classList.add("dummyMenu");
            upperBorder.onmouseenter = () => this.clear();
            this.subMenuCells[parentIndex].upperBorder = upperBorder;
        }
        // Adding lowerBorder: to clear the menu when the cursor hover under last submenu
        if (isLastSubMenu && this.cells.length > subMenuPlacementIdx + 1) {
            const lowerBorder = this.rows[subMenuPlacementIdx + 1].insertCell(1);
            lowerBorder.classList.add("dummyMenu");
            lowerBorder.onmouseenter = () => this.clear();
            this.subMenuCells[parentIndex].lowerBorder = lowerBorder;
        }
    }

    public setSubMenuAction(parentIndex: number, subMenuIndex: number,
        mi: MenuItem, enabled: boolean): void {
        const cell = this.subMenuCells[parentIndex].cells[subMenuIndex];
        if (mi.action != null && enabled) {
            cell.onclick = () => {
                this.hide();
                mi.action();
            };
        } else {
            cell.onclick = () => this.hide();
        }
    }
}

/**
 * A sub-menu is a menu displayed when selecting an item
 * from a topmenu.
 */
export class SubMenu extends BaseMenu<MenuItem> implements IHtmlElement {
    /**
     * Build a submenu.
     * @param {MenuItem[]} mis  List of items to display.
     */
    constructor(mis: MenuItem[]) {
        super();
        this.addItems(mis);
    }

    public show(): void {
        this.outer.classList.remove("hidden");
        this.outer.tabIndex = 1;  // necessary for keyboard events?
    }

    public setAction(mi: MenuItem, enabled: boolean): void {
        const cell = this.getCell(mi);
        if (mi.action != null && enabled)
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); mi.action(); };
        else
            cell.onclick = (e: MouseEvent) => { e.stopPropagation(); this.hide(); };
    }
}

/**
 * A topmenu is composed only of submenus.
 * TODO: allow a topmenu to also have simple items.
 */
export interface TopMenuItem extends BaseMenuItem {
    readonly subMenu: SubMenu;
}

/**
 * A TopMenu is a two-level menu.
 */
export class TopMenu extends BaseMenu<TopMenuItem> {
    /**
     * Create a topmenu.
     * @param {TopMenuItem[]} mis  List of top menu items to display.
     */
    constructor(mis: TopMenuItem[]) {
        super();
        this.outer.classList.add("topMenu");
        this.outer.classList.remove("hidden");
        this.tableBody.insertRow();
        this.addItems(mis);
    }

    public hideSubMenus(): void {
        this.items.forEach((mi) => mi.subMenu.hide());
    }

    public addItem(mi: TopMenuItem, enabled: boolean): HTMLTableDataCellElement {
        const cell = this.tableBody.rows.item(0).insertCell();
        cell.id = makeId(mi.text);  // for testing
        cell.textContent = mi.text;
        cell.appendChild(mi.subMenu.getHTMLRepresentation());
        cell.classList.add("menuItem");
        if (mi.help != null)
            cell.title = mi.help;
        this.items.push(mi);
        this.cells.push(cell);
        this.setAction(mi, enabled);
        return cell;
    }

    public setAction(mi: TopMenuItem, enabled: boolean): void {
        const cell = this.getCell(mi);
        cell.onclick = enabled ? () => {
            this.hideSubMenus();
            cell.classList.add("selected");
            mi.subMenu.show();
        } : () => {
            this.hideSubMenus();
        };
        cell.onmouseleave = () => {
            cell.classList.remove("selected");
            this.hideSubMenus();
        };
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    /**
     * Find the position of an item
     * @param text   Text string in the item.
     */
    public getSubmenu(text: string): SubMenu {
        for (const item of this.items)
            if (item.text === text)
                return item.subMenu;
        return null;
    }
}