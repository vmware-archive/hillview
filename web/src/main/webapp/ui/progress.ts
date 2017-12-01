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

import {ICancellable} from "../util";
import {IHtmlElement} from "./ui";

/**
 * A progressbar displays the amount of work that has been done.
 * A progressbar has a string description, a horizontal progress bar going
 * from 0 to 100% and a `stop` button which can be used to abort the
 * operation.
 */
export class ProgressBar implements IHtmlElement {
    end: number;

    private finished: boolean;
    private bar: HTMLElement;
    private topLevel: HTMLElement;

    /**
     * Create a progressbar.
     * @param {ProgressManager} manager   Manager which displays the progress bar.
     * @param {string} description        A description of the operation that is being tracked.
     * @param {ICancellable} operation    Operation that is being executed.  The operation
     *                                    is cancelled if the user presses the "stop" button
     *                                    associated with the progress bar.  May be null.
     */
    constructor(private manager: ProgressManager,
                public readonly description: string,
                private readonly operation: ICancellable) {
        if (description == null)
            throw "Null label";
        if (manager == null)
            throw "Null ProgressManager";

        this.finished = false;
        let top = document.createElement("table");
        top.className = "noBorder";
        this.topLevel = top;
        let body = top.createTBody();
        let row = body.insertRow();
        row.className = "noBorder";

        let cancelButton = document.createElement("button");
        cancelButton.textContent = "Stop";
        let label = document.createElement("div");
        label.textContent = description;
        label.className = "label";

        let outer = document.createElement("div");
        outer.className = "progressBarOuter";

        this.bar = document.createElement("div");
        this.bar.className = "progressBarInner";

        outer.appendChild(this.bar);

        let labelCell = row.insertCell(0);
        labelCell.appendChild(label);
        labelCell.style.textAlign = "left";
        labelCell.className = "noBorder";

        let barCell = row.insertCell(1);
        barCell.appendChild(outer);
        barCell.className = "noBorder";

        let buttonCell = row.insertCell(2);
        buttonCell.appendChild(cancelButton);
        buttonCell.className = "noBorder";

        this.setPosition(0.0);
        cancelButton.onclick = () => this.cancel();
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    setPosition(end: number): void {
        if (this.finished)
        // One may attempt to update the progress bar
        // even after completion
            return;
        if (end < 0)
            end = 0;
        if (end > 1)
            end = 1;
        if (end < this.end)
            console.log("Progress bar moves backward:" + this.end + " to " + end);
        this.end = end;
        this.computePosition();
    }

    computePosition(): void {
        this.bar.style.width = String(this.end * 100) + "%";
    }

    setFinished(): void {
        if (this.finished)
            return;
        this.setPosition(1.0);
        this.finished = true;
        this.manager.removeProgressBar(this);
    }

    cancel(): void {
        if (this.operation != null)
            this.operation.cancel();
        this.setFinished();
    }
}

/**
 * The progress manager maintains multiple progress bars and takes care of hiding
 * them automatically when the associated operations have completed.
 *
 * The PM has an 'idle' class when there are no operations outstanding.
 */
export class ProgressManager implements IHtmlElement {
    topLevel: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "progressManager";
        this.topLevel.classList.add("idle");
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    newProgressBar(operation: ICancellable, description: string) {
        this.topLevel.classList.remove("idle");
        let p = new ProgressBar(this, description, operation);
        this.topLevel.appendChild(p.getHTMLRepresentation());
        return p;
    }

    removeProgressBar(p: ProgressBar) {
        this.topLevel.removeChild(p.getHTMLRepresentation());
        if (this.topLevel.children.length == 0)
            this.topLevel.classList.add("idle");
    }
}