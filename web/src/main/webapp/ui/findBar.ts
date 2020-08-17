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
    protected findInputBox: HTMLInputElement;
    protected substringsFindCheckbox: HTMLInputElement;
    protected regexFindCheckbox: HTMLInputElement;
    protected caseFindCheckbox: HTMLInputElement;
    protected foundCount: HTMLElement;

    constructor(onClick: (next: boolean, fromTop: boolean) => void,
                onFilter: (() => void) | null) {
        this.visible = false;
        this.topLevel = document.createElement("div");
        this.topLevel.style.margin = "2px";
        this.topLevel.className = "highlight";
        this.topLevel.style.flexDirection = "row";
        this.topLevel.style.display = "none";
        this.topLevel.style.flexWrap = "nowrap";
        this.topLevel.style.justifyContent = "flex-start";
        this.topLevel.style.alignItems = "center";

        const label = document.createElement("label");
        label.innerText = "Find: ";
        this.topLevel.appendChild(label);

        this.findInputBox = document.createElement("input");
        this.topLevel.appendChild(this.findInputBox);
        this.addSpace(1);

        const nextButton = this.topLevel.appendChild(document.createElement("button"));
        nextButton.innerHTML = SpecialChars.downArrowHtml;
        nextButton.onclick = () => onClick(true, false);
        this.addSpace(1);

        const prevButton = this.topLevel.appendChild(document.createElement("button"));
        prevButton.innerHTML = SpecialChars.upArrowHtml;
        prevButton.onclick = () => onClick(false, false);
        this.addSpace(1);

        const topButton = this.topLevel.appendChild(document.createElement("button"));
        topButton.innerText = "Search from top";
        topButton.onclick = () => onClick(true, true);
        this.addSpace(1);

        this.substringsFindCheckbox = this.addCheckbox("Substrings:");
        this.regexFindCheckbox = this.addCheckbox("Regex:");
        this.caseFindCheckbox = this.addCheckbox("Match case:");

        this.foundCount = document.createElement("div");
        this.topLevel.appendChild(this.foundCount);

        const filler = document.createElement("div");
        filler.style.flexGrow = "100";
        this.topLevel.appendChild(filler);

        const filterButton = this.topLevel.appendChild(document.createElement("button"));
        filterButton.innerText = "Keep only matching data";
        filterButton.onclick = () => onFilter != null ? onFilter() : {};
        this.addSpace(1);
        const close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.onclick = () => this.show(false);
        close.title = "Cancel the find.";
        this.topLevel.appendChild(close);
    }

    public setCounts(before: number, after: number): void {
        this.foundCount.textContent = formatNumber(before) + " matching before, " +
            formatNumber(after) + " after";
    }

    public getFilter(): StringFilterDescription {
        return {
            compareValue: this.compareValue(),
            asRegEx: this.isRegEx(),
            asSubString: this.substrings(),
            caseSensitive: this.caseSensitive(),
            complement: false
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
    public highlight(text: string, filter: StringFilterDescription | null): HTMLElement {
        if (!this.visible || filter == null)
            return makeSpan(text, false);

        const find = filter.compareValue;
        if (find == null || find === "")
            return makeSpan(text, false);

        const result = makeSpan(null, false);
        if (filter.asRegEx) {
            const modifier = this.caseSensitive() ? "g" : "ig";
            let regex = new RegExp(find, modifier);
            if (!filter.asSubString)
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
            if (filter.asSubString) {
                let index: number;
                while (true) {
                    if (filter.caseSensitive)
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
                if (filter.caseSensitive) {
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
            this.topLevel.style.display = "flex";
            this.findInputBox.focus();
            this.visible = true;
        } else {
            this.topLevel.style.display = "none";
            this.visible = false;
        }
    }

    private addSpace(num: number): void {
        const span = makeSpan("", false);
        let str: string = "";
        for (let i = 0; i < num; i++)
            str += "&nbsp;";
        span.innerHTML = str;
        this.topLevel.appendChild(span);
    }

    private addCheckbox(stringDesc: string ): HTMLInputElement {
        this.addSpace(2);
        const label = document.createElement("label");
        label.textContent = stringDesc;
        this.topLevel.appendChild(label);
        const checkBox: HTMLInputElement = document.createElement("input");
        checkBox.type = "checkbox";
        this.topLevel.appendChild(checkBox);
        const divEnd: HTMLElement = document.createElement("div");
        divEnd.innerHTML = "&nbsp;&nbsp;";
        this.topLevel.appendChild(divEnd);
        return checkBox;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}
