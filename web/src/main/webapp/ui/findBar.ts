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

import {IHtmlElement, SpecialChars} from "./ui";
import {formatNumber, makeSpan} from "../util";
import {StringFilterDescription} from "../javaBridge";

/**
 * A class that offers user interaction for finding.
 */
export class FindBar implements IHtmlElement {
    public visible: boolean;
    protected topLevel: HTMLDivElement;
    protected contents: HTMLDivElement;
    protected findInputBox: HTMLInputElement;
    protected substringsFindCheckbox: HTMLInputElement;
    protected regexFindCheckbox: HTMLInputElement;
    protected caseFindCheckbox: HTMLInputElement;
    protected foundCount: HTMLElement;
    protected strFilter: StringFilterDescription;  // last search we have performed

    constructor(onClick: (filter: StringFilterDescription, fromTop: boolean) => void) {
        this.visible = false;
        this.strFilter = null;

        this.topLevel = document.createElement("div");
        this.topLevel.style.display = "none";
        this.contents = document.createElement("div");
        this.topLevel.appendChild(this.contents);
        this.contents.className = "highlight";
        this.contents.style.display = "flex";
        this.contents.style.margin = "2px";
        this.contents.style.flexDirection = "row";
        this.contents.style.flexWrap = "nowrap";
        this.contents.style.justifyContent = "flex-start";
        this.contents.style.alignItems = "center";

        const label = document.createElement("label");
        label.innerText = "Find: ";
        this.contents.appendChild(label);

        this.findInputBox = document.createElement("input");
        this.contents.appendChild(this.findInputBox);
        this.addSpace(1);

        const nextButton = this.contents.appendChild(document.createElement("button"));
        nextButton.innerHTML = SpecialChars.downArrowHtml;
        nextButton.onclick = () => onClick(this.getFilter(true), false);
        nextButton.title = "Next match";
        this.addSpace(1);

        const prevButton = this.contents.appendChild(document.createElement("button"));
        prevButton.innerHTML = SpecialChars.upArrowHtml;
        prevButton.onclick = () => onClick(this.getFilter(false), false);
        prevButton.title = "Previous match";
        this.addSpace(1);

        const topButton = this.contents.appendChild(document.createElement("button"));
        topButton.innerText = "Search from top";
        topButton.title = "Search from the beginning of the file";
        topButton.onclick = () => onClick(this.getFilter(true), true);
        this.addSpace(1);

        this.substringsFindCheckbox = this.addCheckbox("Substrings:");
        this.regexFindCheckbox = this.addCheckbox("Regex:");
        this.caseFindCheckbox = this.addCheckbox("Match case:");

        this.foundCount = document.createElement("div");
        this.contents.appendChild(this.foundCount);

        const filler = document.createElement("div");
        filler.style.flexGrow = "100";
        this.contents.appendChild(filler);

        const close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.onclick = () => this.show(false);
        close.title = "Cancel the find.";
        this.contents.appendChild(close);
    }

    public setCounts(before: number, after: number): void {
        this.foundCount.textContent = formatNumber(before) + " matching before, " +
            formatNumber(after) + " after";
    }

    protected getFilter(next: boolean): StringFilterDescription {
        const compareValue = this.compareValue();
        if (compareValue === "") {
            return null;
        }

        let excludeTopRow: boolean;
        // If this filter is unchanged from the previous search we exclude the top row
        if (this.strFilter != null &&
            this.strFilter.compareValue === compareValue &&
            this.strFilter.asRegEx === this.isRegEx() &&
            this.strFilter.asSubString === this.substrings() &&
            this.strFilter.caseSensitive === this.caseSensitive()) {
            excludeTopRow = true; // next search
        } else {
            excludeTopRow = false; // new search
        }
        if (!next)
            excludeTopRow = true;

        return this.strFilter = {
            compareValue: compareValue,
            asRegEx: this.isRegEx(),
            asSubString: this.substrings(),
            caseSensitive: this.caseSensitive(),
            complement: false,
            excludeTopRow: excludeTopRow,
            next: next
        };
    }

    public compareValue(): string {
        return this.findInputBox.value;
    }

    public isRegEx(): boolean {
        return this.regexFindCheckbox.checked;
    }

    public caseSensitive(): boolean {
        return this.caseFindCheckbox.checked;
    }

    public substrings(): boolean {
        return this.substringsFindCheckbox.checked;
    }

    /**
     * Highlight a text according to the current find options.
     */
    public highlight(text: string): HTMLElement {
        if (!this.visible || this.strFilter == null)
            return makeSpan(text, false);

        const find = this.strFilter.compareValue;
        if (find == null || find === "")
            return makeSpan(text, false);

        const result = makeSpan(null, false);
        if (this.strFilter.asRegEx) {
            const modifier = this.caseSensitive() ? "g" : "ig";
            let regex = new RegExp(find, modifier);
            if (!this.strFilter.asSubString)
                regex = new RegExp("^" + find + "$", modifier);
            while (true) {
                const match = regex.exec(text);
                if (match == null) {
                    result.appendChild(makeSpan(text, false));
                    return result;
                }
                result.appendChild(makeSpan(text.substr(0, match.index), false));
                result.appendChild(makeSpan(text.substr(match.index, regex.lastIndex - match.index), true));
                text = text.substr(regex.lastIndex);
            }
        } else {
            if (this.strFilter.asSubString) {
                let index: number;
                while (true) {
                    if (this.strFilter.caseSensitive)
                        index = text.indexOf(find);
                    else
                        index = text.toLowerCase().indexOf(find.toLowerCase());
                    if (index < 0) {
                        result.appendChild(makeSpan(text, false));
                        return result;
                    }
                    result.appendChild(makeSpan(text.substr(0, index), false));
                    result.appendChild(makeSpan(text.substr(index, find.length), true));
                    text = text.substr(index + find.length);
                }
            } else {
                if (this.strFilter.caseSensitive) {
                    if (text === find)
                        return makeSpan(text, true);
                } else {
                    if (text.toLowerCase() === find.toLowerCase())
                        return makeSpan(text, true);
                }
            }
        }
        return makeSpan(text, false);
    }

    public show(show: boolean): void {
        if (show) {
            this.topLevel.style.display = "block";
            this.findInputBox.focus();
            this.visible = true;
        } else {
            this.topLevel.style.display = "none";
            this.visible = false;
            this.strFilter = null;
        }
    }

    private addSpace(num: number): void {
        const span = makeSpan("", false);
        let str: string = "";
        for (let i = 0; i < num; i++)
            str += "&nbsp;";
        span.innerHTML = str;
        this.contents.appendChild(span);
    }

    private addCheckbox(stringDesc: string ): HTMLInputElement {
        this.addSpace(2);
        const label = document.createElement("label");
        label.textContent = stringDesc;
        this.contents.appendChild(label);
        const checkBox: HTMLInputElement = document.createElement("input");
        checkBox.type = "checkbox";
        this.contents.appendChild(checkBox);
        const divEnd: HTMLElement = document.createElement("div");
        divEnd.innerHTML = "&nbsp;&nbsp;";
        this.contents.appendChild(divEnd);
        return checkBox;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}
