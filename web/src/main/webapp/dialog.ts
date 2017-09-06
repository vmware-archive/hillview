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

import {IHtmlElement, KeyCodes} from "./ui"
import {ContentsKind} from "./tableData"

// Represents a field in the dialog. It is just the HTML element, and an optional type.
export class DialogField {
    html: HTMLSelectElement | HTMLInputElement;
    type?: ContentsKind;
}

// Base class for dialog implementations
export class Dialog implements IHtmlElement {
    private container: HTMLDivElement;
    private fieldsDiv: HTMLDivElement;
    public onConfirm: () => void;  // method to be invoked when dialog is closed successfully
    // Stores the input elements and (optionally) their types.
    private fields: Map<string, DialogField> = new Map<string, DialogField>();
    private confirmButton: HTMLButtonElement;

    // Create a dialog with the given name.
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

    public setAction(onConfirm: () => void): void {
        this.onConfirm = onConfirm;
        if (onConfirm != null)
            this.confirmButton.onclick = () => {
                this.hide();
                this.onConfirm();
            };
    }

    // display the menu
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

    // Removes the menu from the DOM
    public hide(): void {
        this.container.remove();
    }

    public getHTMLRepresentation(): HTMLDivElement {
        return this.container;
    }

    // Add a text field with the given internal name, label, and data type.
    // @param fieldName: Internal name. Has to be used when parsing the input.
    // @param labelText: Text in the dialog for this field.
    // @param type: Data type of this field. For now, only Integer is special.
    // @param value: Initial default value.
    public addTextField(fieldName: string, labelText: string, type: ContentsKind, value?: string): void {
        let fieldDiv = document.createElement("div");
        this.fieldsDiv.appendChild(fieldDiv);

        let label = document.createElement("label");
        label.textContent = labelText;
        fieldDiv.appendChild(label);

        let input = <HTMLInputElement> document.createElement("input");
        fieldDiv.appendChild(input);
        if (type == "Integer") {
            input.type = "number";
        }
        this.fields.set(fieldName, {html: input, type: type});
        if (value != null)
            this.fields.get(fieldName).html.value = value;
    }

    // Add a selection field with the given options.
    // @param fieldName: Internal name. Has to be used when parsing the input.
    // @param labelText: Text in the dialog for this field.
    // @param options: List of strings that are the options in the selection box.
    // @param value: Initial default value.
    public addSelectField(fieldName: string, labelText: string, options: string[], value?: string): void {
        let fieldDiv = document.createElement("div");
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
        this.fields.set(fieldName, {html: select});
        if (value != null)
            this.fields.get(fieldName).html.value = value;
    }

    public getFieldValue(field: string): string {
        return this.fields.get(field).html.value;
    }

    public getFieldValueAsInt(field: string): number {
        let s = this.getFieldValue(field);
        return parseInt(s);
    }

    public getFieldValueAsNumber(field: string): number {
        let s = this.getFieldValue(field);
        return parseFloat(s);
    }

    // Remove this element from the DOM.
    private cancelAction(): void {
        this.hide();
    }
}
