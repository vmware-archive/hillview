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

import {ICancellable, IRawCancellable, readableTime} from "../util";
import {IHtmlElement} from "./ui";

/**
 * A progressbar displays the amount of work that has been done.
 * A progressbar has a string description, a horizontal progress bar going
 * from 0 to 100% and a `stop` button which can be used to abort the
 * operation.
 */
export class ProgressBar implements IHtmlElement {
    protected end: number;
    private finished: boolean;
    private readonly bar: HTMLElement;
    private readonly topLevel: HTMLElement;
    private readonly estimate: HTMLElement;
    private firstUpdate: Date;
    private firstPosition: number;

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
                private readonly operation: IRawCancellable) {
        if (description == null)
            throw new Error("Null label");
        if (manager == null)
            throw new Error("Null ProgressManager");
        this.firstUpdate = null;
        this.firstPosition = 0;

        this.finished = false;
        const top = document.createElement("table");
        top.className = "noBorder";
        this.topLevel = top;
        const body = top.createTBody();
        const row = body.insertRow();
        row.className = "noBorder";

        const cancelButton = document.createElement("button");
        cancelButton.textContent = "Stop";
        const label = document.createElement("div");
        label.textContent = description;
        label.className = "progressLabel";

        const outer = document.createElement("div");
        outer.className = "progressBarOuter";

        this.bar = document.createElement("div");
        this.bar.className = "progressBarInner";

        outer.appendChild(this.bar);

        const labelCell = row.insertCell(0);
        labelCell.appendChild(label);
        labelCell.style.textAlign = "left";
        labelCell.className = "noBorder";

        const barCell = row.insertCell(1);
        barCell.appendChild(outer);
        barCell.className = "noBorder";

        const buttonCell = row.insertCell(2);
        buttonCell.appendChild(cancelButton);
        buttonCell.className = "noBorder";

        this.setPosition(0.0);
        cancelButton.onclick = () => this.cancel();

        this.estimate = document.createElement("div");
        const estimateCell = row.insertCell(3);
        estimateCell.className = "noBorder";
        estimateCell.appendChild(this.estimate);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public setPosition(end: number): void {
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
        const time = new Date();
        if (this.firstUpdate == null) {
            this.firstUpdate = time;
            this.firstPosition = end;
        } else {
            const elapsed = time.getTime() - this.firstUpdate.getTime();
            const progress = end - this.firstPosition;
            if (progress > 0 && elapsed > 2000) {
                const estimated = elapsed / progress - elapsed;
                this.estimate.textContent = "Remaining time: " + readableTime(estimated);
            }
        }
        this.end = end;
        this.computePosition();
    }

    public computePosition(): void {
        this.bar.style.width = String(this.end * 100) + "%";
    }

    public setFinished(): void {
        if (this.finished)
            return;
        this.setPosition(1.0);
        this.finished = true;
        this.manager.removeProgressBar(this);
    }

    public cancel(): void {
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
    public topLevel: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "progressManager";
        this.topLevel.classList.add("idle");
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public newProgressBar(operation: IRawCancellable, description: string): ProgressBar {
        this.topLevel.classList.remove("idle");
        const p = new ProgressBar(this, description, operation);
        this.topLevel.appendChild(p.getHTMLRepresentation());
        return p;
    }

    public removeProgressBar(p: ProgressBar): void {
        this.topLevel.removeChild(p.getHTMLRepresentation());
        if (this.topLevel.children.length === 0)
            this.topLevel.classList.add("idle");
    }
}
