/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {IHtmlElement, removeAllChildren} from "./ui";
import {IScrollTarget} from "./scroll";
import {px} from "../util";

export class RangeView implements IHtmlElement {
    protected topLevel: HTMLElement;
    protected cachedStart: number;
    protected cachedEnd: number;
    protected visibleStart: number;
    protected visibleEnd: number;
    protected max: number;
    protected wrap = true;
    protected contents: HTMLElement;
    protected before: HTMLElement;
    protected after: HTMLElement;

    /*  Layout
        --------------------- 0
        |                   |
        |-------------------| cachedStart
        |                   |
        |-------------------| visibleStart
        |                   |
        |-------------------| visibleEnd
        |                   |
        |-------------------| cachedEnd
        |                   |
        |-------------------| max

        0 <= cachedStart <= visibleStart <= visibleEnd <= cachedEnd <= max.
     */
    constructor(protected target: IScrollTarget) {
        this.cachedStart = 0;
        this.visibleStart = 0;
        this.visibleEnd = 0;
        this.cachedEnd = 0;

        this.topLevel = document.createElement("div");
        this.topLevel.className = "logFileContents";
        const container = document.createElement("div");
        container.style.overflow = "auto";
        this.topLevel.appendChild(container);

        this.before = document.createElement("div");
        this.after = document.createElement("div");
        this.before.className = "filler";
        this.after.className = "filler";

        this.contents = document.createElement("div");
        container.appendChild(this.before);
        container.appendChild(this.contents);
        container.appendChild(this.after);
    }

    public setMax(max: number): void {
        this.max = max;
    }

    public display(rows: HTMLElement | null, start: number, count: number): void {
        console.assert(count >= 0);
        console.assert(start + count <= this.max);
        removeAllChildren(this.contents);
        if (rows == null) {
            this.before.style.height = "0";
            this.after.style.height = "0";
            return;
        }
        this.contents.appendChild(rows);
        this.cachedStart = start;
        this.cachedEnd = this.cachedStart + count;
        // We compute some sizes for before and after that place the scrollbar in
        // the approximate correct size.
        const displayedPixels = this.contents.offsetHeight;
        const rowSize = displayedPixels / count;
        let beforeSize = start * rowSize;
        let afterSize = (this.max - start - count) * rowSize;
        const largest = Math.max(beforeSize, afterSize);
        const scaling = largest > 100000 ? largest / 100000 : 1.0;
        beforeSize *= scaling;
        afterSize *= scaling;
        this.before.style.height = px(beforeSize);
        this.after.style.height = px(afterSize);
    }

    public scrollTo(startRow: number): void {

    }

    public toggleWrap(): void {
        this.wrap = !this.wrap;
        if (!this.wrap) {
            this.contents.style.whiteSpace = "nowrap";
        } else {
            this.contents.style.whiteSpace = "normal";
        }
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}
