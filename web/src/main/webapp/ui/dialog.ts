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

import {IHtmlElement, KeyCodes} from "./ui"
import {ContentsKind} from "../tableData"

/**
 *  Represents a field in the dialog.
 */
export class DialogField {
    html: HTMLSelectElement | HTMLInputElement;
    /**
     * Optional kind of data expected to be input by the user.
     */
    type?: ContentsKind;
}

/**
 * Base class for dialog implementations.
 * A dialog asks the user to fill in values for a set of fields.
 */
export class Dialog implements IHtmlElement {
    private container: HTMLDivElement;
    /**
     * The fieldsDiv is a div that contains all the form fields.
     */
    private fieldsDiv: HTMLDivElement;
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
    private line: Map<string, HTMLElement> = new Map<string, HTMLElement>();
    private confirmButton: HTMLButtonElement;

    /**
     * Create a dialog with the given name.
     */
    constructor(title: string) {
        this.onConfirm = null;
        this.container = document.createElement("div");
        this.container.classList.add('dialog');

        let titleElement = document.createElement("h1");
        titleElement.textContent = title;
        this.container.appendChild(titleElement);

        this.fieldsDiv = document.createElement("div");
        this.container.appendChild(this.fieldsDiv);

        let buttonsDiv = document.createElement("div");
        this.container.appendChild(buttonsDiv);
        buttonsDiv.classList.add('cancelconfirm');

        let cancelButton = document.createElement("button");
        cancelButton.onclick = () => this.cancelAction();
        cancelButton.textContent = "Cancel";
        cancelButton.classList.add("cancel");
        buttonsDiv.appendChild(cancelButton);

        this.confirmButton = document.createElement("button");
        this.confirmButton.textContent = "Confirm";
        this.confirmButton.classList.add("confirm");
        buttonsDiv.appendChild(this.confirmButton);
    }

    protected handleKeypress(ev: KeyboardEvent): void {
        if (ev.keyCode == KeyCodes.enter) {
            this.hide();
            this.onConfirm();
        } else if (ev.key == "Escape") {
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

    /**
     * Add a text field with the given internal name, label, and data type.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param type: Data type of this field. For now, only Integer is special.
     * @param value: Initial default value.
     * @return       The input element created for the user to type the text.
     */
    public addTextField(
        fieldName: string, labelText: string, type: ContentsKind, value?: string): HTMLInputElement {
        let fieldDiv = document.createElement("div");
        fieldDiv.style.display = "flex";
        fieldDiv.style.alignItems = "center";
        this.fieldsDiv.appendChild(fieldDiv);

        let label = document.createElement("label");
        label.textContent = labelText;
        fieldDiv.appendChild(label);

        let input: HTMLInputElement = document.createElement("input");
        fieldDiv.appendChild(input);
        if (type == "Integer")
            input.type = "number";
        if (type == "Integer" || type == "Double")
            input.width = "3em";

        this.fields.set(fieldName, {html: input, type: type});
        if (value != null)
            this.fields.get(fieldName).html.value = value;
        this.line.set(fieldName, fieldDiv);
        return input;
    }

    /**
     * Add a drop-down selection field with the given options.
     * @param fieldName: Internal name. Has to be used when parsing the input.
     * @param labelText: Text in the dialog for this field.
     * @param options: List of strings that are the options in the selection box.
     * @param value: Initial default value.
     * @return       A reference to the select html select field.
     */
    public addSelectField(
        fieldName: string, labelText: string, options: string[], value?: string): HTMLSelectElement {
        let fieldDiv = document.createElement("div");
        fieldDiv.style.display = "flex";
        fieldDiv.style.alignItems = "center";
        this.fieldsDiv.appendChild(fieldDiv);

        let label = document.createElement("label");
        label.textContent = labelText;
        fieldDiv.appendChild(label);

        let select = document.createElement("select");
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
        this.fields.set(fieldName, {html: select});
        if (value != null)
            this.fields.get(fieldName).html.value = value;
        this.line.set(fieldName, fieldDiv);
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
