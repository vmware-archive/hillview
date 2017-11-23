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

import {IHtmlElement, KeyCodes} from "./ui";

/**
 * A textarea which allows users to type a program.
 */
export class EditBox implements IHtmlElement {
    topLevel: HTMLTextAreaElement;

    constructor() {
        this.topLevel = document.createElement("textarea");
        this.topLevel.rows = 10;
        this.topLevel.style.flexGrow = "100";
        // The following will prevent these events from going to the parent element
        this.topLevel.onkeydown = e => e.stopPropagation();
        this.topLevel.onkeypress = e => e.stopPropagation();
        this.topLevel.onmousedown = e => e.stopPropagation();
    }

    focus(): void {
        this.topLevel.focus();
    }

    get value(): string {
        return this.topLevel.value;
    }

    set value(value: string) {
        this.topLevel.value = value;
    }

    setTabIndex(index: number) {
        this.topLevel.tabIndex = index;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}