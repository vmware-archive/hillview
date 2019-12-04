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

import {drag as d3drag} from "d3-drag";
import {event as d3event, select as d3select} from "d3-selection";
import {cloneArray, makeId, makeSpan, px} from "../util";
import {EditBox} from "./editBox";
import {IHtmlElement, Point} from "./ui";
import {DisplayName} from "../schemaClass";

export enum FieldKind {
    String,
    Integer,
    Double,
    Boolean,
    Password,
    File,
    Datetime,
    ColumnName
}

/**
 *  Represents a field in the dialog.
 */
export class DialogField {
    public html: HTMLSelectElement | HTMLInputElement | EditBox;
    /**
     * Optional kind of data expected to be input by the user.
     */
    public type?: FieldKind;
}

/**
 * Maps dialog fields to values.  Used to cache previously-filled values
 * in a dialog.
 */
class DialogValues {
    /**
     * Map field name to field value.
     */
   public values: Map<string, string>;

   constructor() { this.values = new Map<string, string>(); }

   public set(field: string, value: string): void {
       this.values.set(field, value);
   }

   public get(field: string): string {
       return this.values.get(field);
   }
}

/**
 * Base class for implementing dialogs.
 */
class DialogBase implements IHtmlElement {
    protected readonly topLevel: HTMLFormElement;
    protected tabIndex: number;
    protected dragging: boolean;
    protected readonly buttonsDiv: HTMLDivElement;
    private dragMousePosition: Point;
    private dialogPosition: ClientRect;
    /**
     * The fieldsDiv is a div that contains all the form fields.
     */
    protected readonly fieldsDiv: HTMLDivElement;
    // True if any dialog is currently visible.
    private static dialogVisible: boolean;

    /**
     * Create a dialog with the given name.
     * @param title; header to show on top of the dialog.
     * @param toolTip: help message to display on mouseover.
     */
    constructor(title: string, toolTip: string) {
        // Tab indexes seem to be global to the whole DOM.
        // That's not good, since having an element with tabindex 2 will be behind all
        // other elements with tabindex 1, no matter where they are in the document.
        // We choose 10 here, and hope that all menu fields are at least consecutive
        // in tab order in the whole DOM.  Probably the right solution is to handle the
        // tab keypress in an event handler.
        this.tabIndex = 10;
        this.dragging = false;
        this.topLevel = document.createElement("form");
        this.topLevel.title = toolTip;
        this.topLevel.classList.add("dialog");
        this.topLevel.style.left = "50%";
        this.topLevel.style.top = "50%";
        this.topLevel.style.transform = "translate(-50%, -50%)";
        DialogBase.dialogVisible = false;

        const titleElement = document.createElement("h1");
        titleElement.textContent = title;
        this.topLevel.appendChild(titleElement);

        this.fieldsDiv = document.createElement("div");
        this.topLevel.appendChild(this.fieldsDiv);

        this.buttonsDiv = document.createElement("div");
        this.topLevel.appendChild(this.buttonsDiv);

        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("end", () => this.dragEnd())
            .on("drag", () => this.dragMove());
        d3select(this.topLevel).call(drag);
    }

    public dragStart(): void {
        this.dragging = true;
        this.dragMousePosition = { x: d3event.x, y: d3event.y };
        this.dialogPosition = this.topLevel.getBoundingClientRect();
        this.topLevel.style.transform = "";
        this.topLevel.style.cursor = "move";
        this.dragMove();  // put it in the right place; changing the transform may move it.
    }

    public dragMove(): void {
        if (!this.dragging)
            return;
        const dx = this.dragMousePosition.x - d3event.x;
        const dy = this.dragMousePosition.y - d3event.y;
        this.topLevel.style.left = px(this.dialogPosition.left - dx);
        this.topLevel.style.top = px(this.dialogPosition.top - dy);
    }

    public dragEnd(): void {
        this.dragging = false;
        this.topLevel.style.cursor = "default";
    }

    public hide(): void {
        // Removes the menu from the DOM
        this.topLevel.remove();
        Dialog.dialogVisible = false;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public show(): void {
        if (Dialog.dialogVisible)
            return;
        document.body.appendChild(this.topLevel);
        // Prevent two dialogs from being shown at the same time.
        Dialog.dialogVisible = true;
    }
}

/**
 * Dialog implementations - can be further subclassed.
 * A dialog asks the user to fill in values for a set of fields.
 */
export class Dialog extends DialogBase {
    /**
     * Method to be invoked when dialog is closed with OK.
     */
    public onConfirm: () => void;
    /**
     * Stores the input elements and (optionally) their types.
     */
    private fields: Map<string, DialogField> = new Map<string, DialogField>();
    /**
     * Maps a field name to the fieldsDiv that contains all the corresponding visual elements.
     */
    private line: Map<string, HTMLElement>;
    private readonly confirmButton: HTMLInputElement;
    /**
     * Optional string which is used as an index in the dialogValueCache to
     * populate a dialog with its previous values.
     */
    private dialogTitle: string;

    /**
     * This is a cache that can map a dialog title to a set of dialog values.
     */
    public static dialogValueCache: Map<string, DialogValues> = new Map<string, DialogValues>();

    /**
     * Create a dialog with the given name.
     * @param title; header to show on top of the dialog.
     * @param toolTip: help message to display on mouseover.
     */
    constructor(title: string, toolTip: string) {
        // Tab indexes seem to be global to the whole DOM.
        // That's not good, since having an element with tabindex 2 will be behind all
        // other elements with tabindex 1, no matter where they are in the document.
        // We choose 10 here, and hope that all menu fields are at least consecutive
        // in tab order in the whole DOM.  Probably the right solution is to handle the
        // tab keypress in an event handler.
        super(title, toolTip);
        this.dialogTitle = null;
        this.line = new Map<string, HTMLElement>();
        this.onConfirm = null;
        const nodrag = d3drag()
            .on("start", () => this.dragEnd());

        const cancelButton = document.createElement("button");
        cancelButton.onclick = () => this.cancelAction();
        cancelButton.textContent = "Cancel";
        cancelButton.classList.add("cancel");
        d3select(cancelButton).call(nodrag);
        this.buttonsDiv.appendChild(cancelButton);

        this.confirmButton = document.createElement("input");
        this.confirmButton.type = "submit";
        this.confirmButton.value = "Confirm";
        this.confirmButton.classList.add("confirm");
        d3select(this.confirmButton).call(nodrag);
        this.buttonsDiv.appendChild(this.confirmButton);
    }

    /**
     * Sets the dialog title.  This indicates that the dialog values
     * will be cached during a session and filled automatically if the
     * dialog appears again.  If a set of values for this title is already
     * available, it is used to populate the dialog.
     */
    public setCacheTitle(title: string | null): void {
        this.dialogTitle = title;
        if (title != null) {
            const values = Dialog.dialogValueCache.get(title);
            if (values != null)
                this.setAllValues(values);
        }
    }

    public getAllValues(): DialogValues {
        const retval = new DialogValues();
        this.fields.forEach((v, k) => retval.set(k, this.getFieldValueChecked(k, false)));
        return retval;
    }

    public setAllValues(values: DialogValues): void {
        this.fields.forEach((v, k) => {
            const value = values.get(k);
            if (value != null)
                this.setFieldValue(k, value);
        });
    }

    protected handleKeypress(ev: KeyboardEvent): void {
        if (ev.code === "Enter") {
            this.hide();
            this.cacheValues();
            this.onConfirm();
        } else if (ev.code === "Escape") {
            this.hide();
        }
    }

    public cacheValues(): void {
        if (this.dialogTitle == null)
            return;
        const vals = this.getAllValues();
        Dialog.dialogValueCache.set(this.dialogTitle, vals);
    }

    /**
     * Set the action to execute when the dialog is closed.
     * @param {() => void} onConfirm  Action to execute.
     */
    public setAction(onConfirm: () => void): void {
        this.onConfirm = onConfirm;
        if (onConfirm != null)
            this.confirmButton.onclick = () => {
                if (this.topLevel.checkValidity()) {
                    this.hide();
                    this.cacheValues();
                    this.onConfirm();
                }
            };
    }

    /**
     * Display the dialog.
     */
    public show(): void {
        super.show();
        if (this.fieldsDiv.childElementCount === 0) {
            // If there are somehow no fields, focus on the container.
            this.topLevel.setAttribute("tabindex", "10");
            this.topLevel.focus();
        } else {
            // Focus on the first input element if present
            const firstField = this.fields.values().next();
            if (firstField.value)
                firstField.value.html.focus();
            else
                this.topLevel.focus();
        }
        this.topLevel.onkeydown = (ev) => this.handleKeypress(ev);
    }

    private createRowContainer(fieldName: string, labelText: string, toolTip: string): HTMLDivElement {
        const fieldDiv = document.createElement("div");
        fieldDiv.style.display = "flex";
        fieldDiv.style.alignItems = "center";
        fieldDiv.title = toolTip;
        fieldDiv.onmousedown = (e) => e.stopPropagation();
        this.fieldsDiv.appendChild(fieldDiv);
        const label = document.createElement("label");
        label.textContent = labelText;
        fieldDiv.appendChild(label);
        this.line.set(fieldName, fieldDiv);
        return fieldDiv;
    }

    private createInputElement(fieldName: string, labelText: string,
                               toolTip: string, type: string): HTMLInputElement {
        const fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        if (type === "checkbox") {
            const filler = document.createElement("span");
            filler.style.flexGrow = "100";
            fieldDiv.appendChild(filler);
        }
        const input: HTMLInputElement = document.createElement("input");
        input.tabIndex = this.tabIndex++;
        if (type !== "checkbox")
            input.style.flexGrow = "100";
        input.id = makeId(fieldName);
        input.type = type;
        fieldDiv.appendChild(input);
        return input;
    }

    /**
     * Add a text field with the given internal name, label, and data type.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param type: Data type of this field.
     * @param value: Initial default value.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       The input element created for the user to type the text.
     */
    public addTextField(fieldName: string, labelText: string,
                        type: FieldKind, value: string,
                        toolTip: string): HTMLInputElement {
        let t = "string";
        if (type === FieldKind.Integer)
            t = "number";
        if (type === FieldKind.Password)
            t = "password";
        const input = this.createInputElement(fieldName, labelText, toolTip, t);
        this.fields.set(fieldName, {html: input, type});
        if (value != null)
            input.value = value;
        return input;
    }

    /**
     * Add a field used to select a file name on the local filesystem.
     * @param {string} fieldName   Name of the field.
     * @param {string} labelText   Text in the dialog for this field.
     * @param {string} toolTip     Help message to show as a tooltip.
     * @return       The input element created for the user to type the text.
     */
    public addFileField(
        fieldName: string, labelText: string, toolTip: string): HTMLInputElement {
        const input = this.createInputElement(fieldName, labelText, toolTip, "file");
        this.fields.set(fieldName, {html: input, type: FieldKind.File});
        return input;
    }

    /**
     * Add a multi-line text field with the given internal name, label, and data type.
     * @param fieldName Internal name. Has to be used when parsing the input.
     * @param labelText Text in the dialog for this field.
     * @param pre       String to write before editable field.
     * @param value     Initial default value.
     * @param post      String to write after editable field.
     * @param toolTip   Help message to show as a tool-tip.
     */
    public addMultiLineTextField(fieldName: string, labelText: string, pre: string,
                                 value: string, post: string, toolTip: string): void {
        const fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        const input = new EditBox(fieldName, pre, value, post);
        input.setTabIndex(this.tabIndex++);
        fieldDiv.appendChild(input.getHTMLRepresentation());
        this.fields.set(fieldName, {html: input});
    }

    /**
     * Adds a simple text to the dialog.
     * @param textString  Text to show in the dialog.
     * @returns           A reference to the HTML element holding the text.
     */
    public addText(textString: string): HTMLElement {
        const fieldDiv = this.createRowContainer("message", textString, null);
        return fieldDiv.children[0] as HTMLElement;
    }

    /**
     * Add a text field with the given internal name, label, and data type.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param value: Initial default value.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       The input element created for the user to type the text.
     */
    public addBooleanField(fieldName: string, labelText: string,
                           value: boolean, toolTip: string): HTMLInputElement {
        const input = this.createInputElement(fieldName, labelText, toolTip, "checkbox");
        this.fields.set(fieldName, {html: input, type: FieldKind.Boolean });
        if (value != null && value)
            input.checked = true;
        return input;
    }

    /**
     * Add a datetime-local input field.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param value: Initial default value.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       The input element created for the user to type the text.
     */
    public addDateTimeField(fieldName: string, labelText: string,
                            value: Date | null,
                            toolTip: string): HTMLInputElement {
        const input = this.createInputElement(fieldName, labelText, toolTip, "datetime-local");
        this.fields.set(fieldName, { html: input, type: FieldKind.Datetime });
        if (value != null)
            input.value = value.toISOString().slice(0, 16);
        return input;
    }

    private addSelectInternal(fieldName: string, labelText: string,
                              options: string[], value: string,
                              toolTip: string, type: FieldKind): HTMLInputElement | HTMLSelectElement {
        let result: HTMLInputElement | HTMLSelectElement = null;

        // If we have lots of options use a sorted datalist, otherwise
        // use a select field.
        if (options.length > 5) {
            const select = this.createInputElement(fieldName, labelText, toolTip, "string");
            const sortOptions = cloneArray(options);
            sortOptions.sort();
            const datalist = document.createElement("datalist");
            this.topLevel.appendChild(datalist);
            datalist.id = makeId("list" + fieldName);
            select.setAttribute("list", datalist.id);
            sortOptions.forEach((option) => {
                const optionElement = document.createElement("option");
                optionElement.value = option;
                optionElement.text = option;
                datalist.appendChild(optionElement);
            });
            result = select;
        } else {
            const fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
            const select = document.createElement("select");
            select.tabIndex = this.tabIndex++;
            select.style.flexGrow = "100";
            select.id = makeId(fieldName);
            fieldDiv.appendChild(select);

            options.forEach((option) => {
                const optionElement = document.createElement("option");
                optionElement.value = option;
                optionElement.text = option;
                select.add(optionElement);
            });

            if (value != null) {
                let i;
                for (i = 0; i < options.length; i++) {
                    if (options[i] === value) {
                        select.selectedIndex = i;
                        break;
                    }
                }
                if (i === options.length)
                    throw new Error(`Given default value ${value} not found in options.`);
            }
            result = select;
        }

        this.fields.set(fieldName, {html: result, type: type });
        if (value != null)
            result.value = value;
        return result;
    }

    /**
     * Add a drop-down selection field with the given options.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param options: List of strings that are the options in the selection box.
     * @param value: Initial default value.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       A reference to the select html input field.
     */
    public addSelectField(fieldName: string, labelText: string,
                          options: string[], value: string,
                          toolTip: string): HTMLInputElement | HTMLSelectElement {
        return this.addSelectInternal(
            fieldName, labelText, options, value, toolTip, FieldKind.String);
    }

    /**
     * Add a drop-down selection field for column names with the given options.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param options: List of column display names that are the options in the selection box.
     * @param value: Initial default value; if null the first value from the options is used.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       A reference to the select html input field.
     */
    public addColumnSelectField(fieldName: string, labelText: string,
                                options: DisplayName[], value: DisplayName,
                                toolTip: string): HTMLInputElement | HTMLSelectElement {
        let v = null;
        if (value !== null)
            v = value.displayName;
        else if (options.length > 0)
            v = options[0].displayName;
        return this.addSelectInternal(
            fieldName, labelText, options.map((c) => c.displayName),
            v, toolTip, FieldKind.ColumnName);
    }

    /**
     * Make the specified field visible or invisible.
     * @param {string} labelText   Label associated to the field.
     * @param {boolean} show       If true show the field, else hide it.
     */
    public showField(labelText: string, show: boolean): void {
        const fieldDiv = this.line.get(labelText);
        if (fieldDiv == null)
            return;
        if (show)
            fieldDiv.style.display = "flex";
        else
            fieldDiv.style.display = "none";
    }

    private getFieldValueChecked(field: string, check: boolean): string {
        const f = this.fields.get(field);
        if (f.type === FieldKind.Boolean) {
            const hi = f.html as HTMLInputElement;
            return "" + hi.checked;
        } else {
            if (check)
                console.assert(f.type !== FieldKind.ColumnName);
            return f.html.value;
        }
    }

    /**
     * The value associated with a specific field in a dialog.
     * @param {string} field  Field whose value is sought.
     * @returns {string}      The value associated to the given field.
     */
    public getFieldValue(field: string): string {
        return this.getFieldValueChecked(field, true);
    }

    /**
     * Get the value associated with a field that represents a column name.
     * @param field  Field name.
     */
    public getColumnName(field: string): DisplayName {
        const f = this.fields.get(field);
        console.assert(f.type === FieldKind.ColumnName);
        return new DisplayName(f.html.value);
    }

    /**
     * The value associated with a boolean (checkbox) field.
     * @param {string} field  Field name whose value is sought.
     * @returns {boolean}  True if the field is checked.
     */
    public getBooleanValue(field: string): boolean {
        return (this.fields.get(field).html as HTMLInputElement).checked;
    }

    /**
     * The value associated with a datetime field.
     * @param {string} field  Field name whose value is sought.
     */
    public getDateTimeValue(field: string): Date | null {
        return new Date((this.fields.get(field).html as HTMLInputElement).value);
    }

    /**
     * Set the value of a field in the dialog.
     * @param {string} field  Field whose value is set.
     * @param {string} value  Value that is being set.
     */
    public setFieldValue(field: string, value: string): void {
        let f = this.fields.get(field);
        if (f.type === FieldKind.Boolean) {
            const hi = f.html as HTMLInputElement;
            hi.checked = (value === "true");
        } else {
            f.html.value = value;
        }
    }

    /**
     * Set the value of a column field in the dialog.
     * @param {string} field  Field whose value is set.
     * @param value  Value that is being set.
     */
    public setColumnValue(field: string, value: DisplayName): void {
        const f = this.fields.get(field);
        console.assert(f.type === FieldKind.ColumnName);
        f.html.value = value.displayName;
    }

    /**
     * The value associated with a field cast to an integer.
     * Returns either a number or null if the value cannot be parsed.
     */
    public getFieldValueAsInt(field: string): number {
        const s = this.getFieldValue(field);
        const result = parseInt(s, 10);
        if (isNaN(result))
            return null;
        return result;
    }

    /**
     * The value associated with a field cast to a double.
     * Returns either a number or null if the value cannot be parsed.
     */
    public getFieldValueAsNumber(field: string): number {
        const s = this.getFieldValue(field);
        const result = parseFloat(s);
        if (isNaN(result))
            return null;
        return result;
    }

    /**
     * The value associated with a field representing a file selector.
     */
    public getFieldValueAsFiles(field: string): FileList {
        const f = this.fields.get(field).html as HTMLInputElement;
        return f.files;
    }

    private cancelAction(): void {
        // Remove this element from the DOM.
        this.hide();
    }
}

/**
 * Notifications have no input elements, just an OK button.
 */
export class NotifyDialog extends DialogBase {
    constructor(title: string, message: string | null, toolTip: string) {
        super(title, toolTip);

        const nodrag = d3drag()
            .on("start", () => this.dragEnd());

        const confirmButton = document.createElement("button");
        confirmButton.textContent = "Confirm";
        confirmButton.classList.add("confirm");
        confirmButton.onclick = () => this.hide();
        d3select(confirmButton).call(nodrag);
        this.buttonsDiv.appendChild(confirmButton);

        if (message != null)
            this.fieldsDiv.appendChild(makeSpan(message, false));
    }

    public show(): void {
        super.show();
        this.topLevel.focus();
        this.topLevel.onkeydown = (ev) => this.handleKeypress(ev);
    }

    protected handleKeypress(ev: KeyboardEvent): void {
        if (ev.code === "Enter" || ev.code === "Escape")
            this.hide();
    }
}
