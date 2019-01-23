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
import {px, visible} from "../util";
import {LogFileView} from "../dataViews/logFileView";

/**
 * An interface which allows a view of a big file to move to a specific area.
 */
export interface IUpdate {
    /**
     * Download the specified rows.
     * @param start    First row to download.
     * @param count    Number of rows to download.
     * @param firstRow First row to display on screen after download.
     */
    download(start: number, count: number, firstRow: number): void;
}

export class RangeView implements IHtmlElement {
    protected topLevel: HTMLElement;
    protected cachedStart: number;
    public firstRow: number;
    protected cachedEnd: number;
    protected max: number;
    protected wrap = true;
    protected contents: HTMLElement;
    protected before: HTMLElement;
    protected after: HTMLElement;
    protected scrollTimer: number = null;
    protected downloading: HTMLElement;

    /*  Layout
        |-------------------| 0
        |                   |
        |-------------------| cachedStart
        |                   |
        |//// visible //////|
        |                   |
        |-------------------| cachedEnd
        |                   |
        |-------------------| max

        0 <= cachedStart <= visibleStart <= visibleEnd <= cachedEnd <= max.
     */
    constructor(protected target: IUpdate) {
        this.cachedStart = 0;
        this.firstRow = 0;
        this.cachedEnd = 0;

        this.topLevel = document.createElement("div");
        this.topLevel.className = "logFileContents";

        this.before = document.createElement("div");
        this.after = document.createElement("div");
        this.before.className = "filler";
        this.after.className = "filler";

        this.contents = document.createElement("div");
        this.topLevel.appendChild(this.before);
        this.topLevel.appendChild(this.contents);
        this.topLevel.appendChild(this.after);
        this.topLevel.onscroll = () => this.scrolled();
    }

    public setMax(max: number): void {
        this.max = max;
    }

    private scrolled(): void {
        // Calls scrollStopped after a timeout.
        if (this.scrollTimer != null)
            clearTimeout(this.scrollTimer);
        this.scrollTimer = setTimeout(() => this.scrollStopped(), 100);
    }

    public rowsVisible(): [number, number, number] {
        let firstVisible = null;
        let lastVisible = null;
        const interpolated = Math.round(
            this.topLevel.scrollTop * this.max / this.topLevel.scrollHeight);

        let index = this.cachedStart;
        this.contents.childNodes.forEach((c) => {
            if (visible(c as HTMLElement)) {
                if (firstVisible == null)
                    firstVisible = index;
                lastVisible = index;
            } else if (firstVisible != null) {
                // we found them all
                return [firstVisible, lastVisible, interpolated];
            }
            index++;
        });

        return [firstVisible, lastVisible, interpolated];
    }

    /**
     * Called when we haven't scrolled for a while.
     */
    private scrollStopped(): void {
        // We want to find the first and last rows that are visible.
        const close = 200;
        const [firstVisible, lastVisible, interpolated] = this.rowsVisible();
        console.log("firstVisible " + firstVisible +
            ", lastVisible " + lastVisible +
            ", interpolated " + interpolated);

        if (firstVisible != null) {
            // Something is visible, just patch
            if (this.cachedStart > 0 &&  // something not shown exists
                (firstVisible === this.cachedStart ||
                 firstVisible < this.cachedStart + close)) {
                // Close to beginning: bring more rows to prepend
                const start = Math.max(firstVisible - LogFileView.requestSize, 0);
                this.target.download(start, this.cachedStart - start, -1);
                return;
            } else if (this.cachedEnd < this.max &&
                (lastVisible === this.cachedEnd ||
                 firstVisible > this.cachedEnd - close)) {
                // Close to end: bring more rows to append
                const start = this.cachedEnd + 1;
                const count = Math.min(this.max - start, LogFileView.requestSize);
                this.target.download(start, count, -1);
                return;
            }
            return;
        }
        {
            // we are not seeing anything, bring data around the
            console.assert(interpolated != null);
            const start = Math.max(interpolated - (LogFileView.requestSize / 2), 0);
            this.downloading = document.createElement("div");
            this.downloading.className = "downloading";
            this.downloading.textContent = "Downloading...";
            this.topLevel.appendChild(this.downloading);
            this.target.download(start,
                Math.min(LogFileView.requestSize, this.max - start),
                interpolated);
        }
    }

    /**
     * We have received some more data.  Display it.
     * @param rows      Data that we have received.
     * @param start     Row number of first data element.
     * @param firstRow  First row to display on screen.  If -1 nothing is changed.
     */
    public downloaded(rows: HTMLElement[] | null, start: number, firstRow: number): void {
        const count = rows == null ? 0 : rows.length;
        if (this.downloading != null) {
            this.topLevel.removeChild(this.downloading);
            this.downloading = null;
        }
        console.assert(start + count <= this.max);
        if (rows == null) {
            this.before.style.height = "0";
            this.after.style.height = "0";
            return;
        }

        let freshView = false;  // True if we are not updating an old view
        if (start + count === this.cachedStart) {
            const first = this.contents.childNodes[0];
            for (const r of rows)
                this.contents.insertBefore(r, first);
            this.cachedStart = start;
            // TODO: adjust position to keep same row into view
        } else if (start === this.cachedEnd + 1) {
            this.cachedEnd = this.cachedEnd + count;
            for (const r of rows)
                this.contents.appendChild(r);
        } else {
            removeAllChildren(this.contents);
            for (const r of rows)
                this.contents.appendChild(r);
            this.cachedStart = start;
            this.cachedEnd = this.cachedStart + count;
            freshView = true;
        }

        this.resize(freshView);
        if (firstRow >= 0) {
            const childIndex = firstRow - this.cachedStart;
            if (this.contents.childNodes.length >= childIndex)
                (this.contents.childNodes[childIndex] as HTMLElement).scrollIntoView(false);
        }
    }

    protected resize(freshView: boolean): void {
        const count = this.cachedEnd - this.cachedStart;
        let beforeSize = 10000;
        let afterSize = 10000;
        const displayedPixels = this.contents.offsetHeight;
        if (count !== 0) {
            // We compute some sizes for before and after that place the scrollbar in
            // the approximate correct size.
            const rowSize = displayedPixels / count;
            beforeSize = this.cachedStart * rowSize;
            afterSize = (this.max - this.cachedEnd) * rowSize;
            const largest = Math.max(beforeSize, afterSize);
            const scaling = largest > 100000 ? largest / 100000 : 1.0;
            beforeSize /= scaling;
            afterSize /= scaling;
        }
        this.before.style.height = px(beforeSize);
        this.after.style.height = px(afterSize);
        if (freshView)
            this.topLevel.scrollTop = beforeSize;
        console.log("cached " + this.cachedStart + " " + this.cachedEnd +
            " displayed " + displayedPixels +
            " before " + beforeSize +
            " after " + afterSize);
    }

    public toggleWrap(): void {
        this.wrap = !this.wrap;
        if (!this.wrap) {
            this.contents.style.whiteSpace = "nowrap";
        } else {
            this.contents.style.whiteSpace = "normal";
        }
        this.resize(false);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}
