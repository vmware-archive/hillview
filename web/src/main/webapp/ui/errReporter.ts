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

/**
 * Core interface for reporting errors.
 */
export interface ErrorReporter {
    /**
     * Report an error.
     * @param {string} htmlMessage   Error message as a HTML string.
     */
    reportError(htmlMessage: string) : void;
    /**
     * Clear all displayed error messages.
     * (May do nothing for some implementations, such as a console).
     */
    clear(): void;
}

/**
 * An error reporter that writes messages to the JavaScript browser console.
 */
export class ConsoleErrorReporter implements ErrorReporter {
    public static instance: ConsoleErrorReporter = new ConsoleErrorReporter();

    public reportError(message: string) : void {
        console.log(message);
    }

    public clear() : void {
        // We cannot clear the console
    }
}

/**
 * This class is used to display error messages in the browser window.
 * TODO: rename it, since "console" is not appropriate.
 */
export class ConsoleDisplay implements IHtmlElement, ErrorReporter {
    topLevel: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "console";
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public reportError(htmlMessage: string): void {
        this.topLevel.innerHTML += htmlMessage + "<br>";
    }

    public clear(): void {
        this.topLevel.innerHTML = "";
    }
}
