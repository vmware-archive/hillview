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

import {RemoteObjectId} from "../javaBridge";
import {FullPage, PageTitle} from "../ui/fullPage";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, BigTableView} from "../tableTarget";
import {ICancellable} from "../util";

export class LogFileView extends BigTableView {
    protected readonly topLevel: HTMLElement;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass) {
        super(remoteObjectId, rowCount, schema, null, "LogFileView");

        this.topLevel = document.createElement("div");
        this.topLevel.className = "logFileViewer";
        const menuBar = document.createElement("div");
        menuBar.className = "logFileMenu";
        this.topLevel.appendChild(menuBar);
        const header = document.createElement("div");
        header.className = "logFileHeader";
        this.topLevel.appendChild(header);
        const contentsBar = document.createElement("div");
        contentsBar.className = "logFileContents";
        this.topLevel.appendChild(contentsBar);
        const footer = document.createElement("div");
        footer.className = "logFileFooter";
        this.topLevel.appendChild(footer);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public getPage(): FullPage {
        return null;
    }

    public refresh(): void {
        // TODO
    }

    public resize(): void {
        // TODO
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer {
        return null;  // unused.
    }
}
