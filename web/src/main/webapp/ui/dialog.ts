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

import {d3} from "./d3-modules";
import {IHtmlElement, Point} from "./ui"
import {EditBox} from "./editBox";

export enum FieldKind {
    String,
    Integer,
    Double,
    Boolean,
    Password
}

/**
 *  Represents a field in the dialog.
 */
export class DialogField {
    html: HTMLSelectElement | HTMLInputElement | EditBox;
    /**
     * Optional kind of data expected to be input by the user.
     */
    type?: FieldKind;
}

/**
 * Base class for dialog implementations.
 * A dialog asks the user to fill in values for a set of fields.
 */
export class Dialog implements IHtmlElement {
    private tabIndex: number;
    private container: HTMLDivElement;
    /**
     * The fieldsDiv is a div that contains all the form fields.
     */
    private fieldsDiv: HTMLDivElement;
    /**
     * Method to be invoked when dialog is closed with OK.
     */
    onConfirm: () => void;
    /**
     * Stores the input elements and (optionally) their types.
     */
    private fields: Map<string, DialogField> = new Map<string, DialogField>();
    /**
     * Maps a field name to the fieldsDiv that contains all the corresponding visual elements.
     */
    private line: Map<string, HTMLElement> = new Map<string, HTMLElement>();
    private confirmButton: HTMLButtonElement;
    private dragMousePosition: Point;
    private dialogPosition: ClientRect;

    /**
     * Create a dialog with the given name.
     * @param title; header to show on top of the dialog.
     * @param toolTip: help message to display on mouseover.
     */
    constructor(title: string, toolTip: string) {
        this.tabIndex = 0;
        this.onConfirm = null;
        this.container = document.createElement("div");
        this.container.title = toolTip;
        this.container.classList.add('dialog');
        this.container.style.left = "50%";
        this.container.style.top = "50%";
        this.container.style.transform = "translate(-50%, -50%)";

        let titleElement = document.createElement("h1");
        titleElement.textContent = title;
        this.container.appendChild(titleElement);

        this.fieldsDiv = document.createElement("div");
        this.container.appendChild(this.fieldsDiv);

        let buttonsDiv = document.createElement("div");
        this.container.appendChild(buttonsDiv);

        let cancelButton = document.createElement("button");
        cancelButton.onclick = () => this.cancelAction();
        cancelButton.textContent = "Cancel";
        cancelButton.classList.add("cancel");
        buttonsDiv.appendChild(cancelButton);

        this.confirmButton = document.createElement("button");
        this.confirmButton.textContent = "Confirm";
        this.confirmButton.classList.add("confirm");
        buttonsDiv.appendChild(this.confirmButton);

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("end", () => this.dragEnd())
            .on("drag", () => this.dragMove());
        d3.select(this.container).call(drag);
    }

    dragStart(): void {
        this.dragMousePosition = { x: d3.event.x, y: d3.event.y };
        this.dialogPosition = this.container.getBoundingClientRect();
        this.container.style.transform = "";
        this.container.style.cursor = "move";
        this.dragMove();  // put it in the right place; changing the transform may move it.
    }

    dragMove(): void {
        let dx = this.dragMousePosition.x - d3.event.x;
        let dy = this.dragMousePosition.y - d3.event.y;
        this.container.style.left = (this.dialogPosition.left - dx).toString() + "px";
        this.container.style.top = (this.dialogPosition.top - dy).toString() + "px";
    }

    dragEnd(): void {
        this.container.style.cursor = "default";
    }

    protected handleKeypress(ev: KeyboardEvent): void {
        if (ev.code == "Enter") {
            this.hide();
            this.onConfirm();
        } else if (ev.code == "Escape") {
            this.hide();
        }
    }

    /**
     * Set the action to execute when the dialog is closed.
     * @param {() => void} onConfirm  Action to execute.
     */
    public setAction(onConfirm: () => void): void {
        this.onConfirm = onConfirm;
        if (onConfirm != null)
            this.confirmButton.onclick = () => {
                this.hide();
                this.onConfirm();
            };
    }

    /**
     * Display the menu
      */
    public show(): void {
        document.body.appendChild(this.container);
        if (this.fieldsDiv.childElementCount == 0) {
            // If there are somehow no fields, focus on the container.
            this.container.setAttribute("tabindex", "0");
            this.container.focus();
        } else {
            // Focus on the first input element.
            this.fields.values().next().value.html.focus();
        }
        this.container.onkeydown = (ev) => this.handleKeypress(ev);
    }

    public hide(): void {
        // Removes the menu from the DOM
        this.container.remove();
    }

    public getHTMLRepresentation(): HTMLDivElement {
        return this.container;
    }

    createRowContainer(fieldName: string, labelText: string, toolTip: string): HTMLDivElement {
        let fieldDiv = document.createElement("div");
        fieldDiv.style.display = "flex";
        fieldDiv.style.alignItems = "center";
        fieldDiv.title = toolTip;
        fieldDiv.onkeydown = e => e.stopPropagation();
        fieldDiv.onkeypress = e => e.stopPropagation();
        fieldDiv.onmousedown = e => e.stopPropagation();
        this.fieldsDiv.appendChild(fieldDiv);

        let label = document.createElement("label");
        label.textContent = labelText;
        fieldDiv.appendChild(label);
        this.line.set(fieldName, fieldDiv);
        return fieldDiv;
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
        let fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        let input: HTMLInputElement = document.createElement("input");
        input.tabIndex = this.tabIndex++;
        input.style.flexGrow = "100";
        fieldDiv.appendChild(input);
        if (type == FieldKind.Integer)
            input.type = "number";
        if (type == FieldKind.Password)
            input.type = "password";

        this.fields.set(fieldName, {html: input, type: type});
        if (value != null)
            input.value = value;
        return input;
    }

    /**
     * Add a multi-line text field with the given internal name, label, and data type.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param pre:   String to write before editable field.
     * @param value: Initial default value.
     * @param post:  String to write after editable field.
     * @param toolTip:  Help message to show as a tool-tip.
     */
    public addMultiLineTextField(fieldName: string, labelText: string, pre: string,
                                 value: string, post: string, toolTip: string): void {
        let fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        let input = new EditBox(pre, value, post);
        input.setTabIndex(this.tabIndex++);
        fieldDiv.appendChild(input.getHTMLRepresentation());

        this.fields.set(fieldName, {html: input});
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
        let fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        let input: HTMLInputElement = document.createElement("input");
        input.tabIndex = this.tabIndex++;
        input.type = "checkbox";
        input.style.flexGrow = "100";
        fieldDiv.appendChild(input);
        this.fields.set(fieldName, {html: input});
        if (value != null && value)
            input.checked = true;
        return input;
    }

    /**
     * Add a drop-down selection field with the given options.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param options: List of strings that are the options in the selection box.
     * @param value: Initial default value.
     * @param toolTip:  Help message to show as a tool-tip.
     * @return       A reference to the select html select field.
     */
    public addSelectField(fieldName: string, labelText: string,
                          options: string[], value: string,
                          toolTip: string): HTMLSelectElement {
        let fieldDiv = this.createRowContainer(fieldName, labelText, toolTip);
        let select = document.createElement("select");
        select.tabIndex = this.tabIndex++;
        select.style.flexGrow = "100";
        fieldDiv.appendChild(select);
        options.forEach(option => {
            let optionElement = document.createElement("option");
            optionElement.value = option;
            optionElement.text = option;
            select.add(optionElement);
        });

        if (value != null) {
            let i;
            for (i = 0; i < options.length; i++) {
                if (options[i] == value) {
                    select.selectedIndex = i;
                    break;
                }
            }
            if (i == options.length)
                throw new Error(`Given default value ${value} not found in options.`);
        }
        this.fields.set(fieldName, {html: select, type: FieldKind.String });
        if (value != null)
            select.value = value;
        return select;
    }

    /**
     * Make the specified field visible or invisible.
     * @param {string} labelText   Label associated to the field.
     * @param {boolean} show       If true show the field, else hide it.
     */
    public showField(labelText: string, show: boolean): void {
        let fieldDiv = this.line.get(labelText);
        if (fieldDiv == null)
            return;
        if (show)
            fieldDiv.style.display = "flex";
        else
            fieldDiv.style.display = "none";
    }

    /**
     * The value associated with a specific field in a dialog.
     * @param {string} field  Field whose value is sought.
     * @returns {string}      The value associated to the given field.
     */
    public getFieldValue(field: string): string {
        return this.fields.get(field).html.value;
    }

    /**
     * The value associated with a boolean (checkbox) field.
     * @param {string} field  Field name whose value is sought.
     * @returns {boolean}  True if the field is checked.
     */
    public getBooleanValue(field: string): boolean {
        return (<HTMLInputElement>this.fields.get(field).html).checked;
    }

    /**
     * Set the value of a field in the dialog.
     * @param {string} field  Field whose value is set.
     * @param {string} value  Value that is being set.
     */
    public setFieldValue(field: string, value: string): void {
        this.fields.get(field).html.value = value;
    }

    /**
     * The value associated with a field cast to an integer.
     * Returns either a number or null if the value cannot be parsed.
     */
    public getFieldValueAsInt(field: string): number {
        let s = this.getFieldValue(field);
        let result = parseInt(s);
        if (isNaN(result))
            return null;
        return result;
    }

    /**
     * The value associated with a field cast to a double.
     * Returns either a number or null if the value cannot be parsed.
     */
    public getFieldValueAsNumber(field: string): number {
        let s = this.getFieldValue(field);
        let result = parseFloat(s);
        if (isNaN(result))
            return null;
        return result;
    }

    private cancelAction(): void {
        // Remove this element from the DOM.
        this.hide();
    }
}
