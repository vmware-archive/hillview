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
 * A textarea which allows users to type a program.
 */
export class EditBox implements IHtmlElement {
    topLevel: HTMLDivElement;
    textArea: HTMLTextAreaElement;
    pre: HTMLInputElement;
    post: HTMLInputElement;

    constructor(name: string, pre: string, value: string, post: string) {
        this.topLevel = document.createElement("div");
        this.textArea = document.createElement("textarea");
        this.textArea.style.fontFamily = "monospace";
        this.topLevel.style.display = "flex";
        this.topLevel.style.flexDirection = "column";

        this.pre = document.createElement("input");
        this.pre.readOnly = true;
        this.pre.style.fontFamily = "monospace";

        this.post = document.createElement("input");
        this.post.readOnly = true;
        this.post.style.fontFamily = "monospace";

        this.textArea.rows = 10;
        this.textArea.style.flexGrow = "100";
        this.textArea.id = makeId(name);
        // The following will prevent these events from going to the parent element
        this.textArea.onkeydown = e => e.stopPropagation();
        this.textArea.onkeypress = e => e.stopPropagation();
        this.textArea.onmousedown = e => e.stopPropagation();

        this.topLevel.appendChild(this.pre);
        this.topLevel.appendChild(this.textArea);
        this.topLevel.appendChild(this.post);
        if (pre != null)
            this.pre.value = pre;
        if (value != null)
            this.value = value;
        if (post != null)
            this.post.value = post;
    }

    focus(): void {
        this.textArea.focus();
    }

    get value(): string {
        return this.textArea.value;
    }

    set value(value: string) {
        this.textArea.value = value;
    }

    setTabIndex(index: number) {
        this.topLevel.tabIndex = index;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}