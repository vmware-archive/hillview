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

import {FullPage} from "./fullPage";
import {IHtmlElement, ViewKind} from "./ui";

/**
 * Main interface implemented by all Hillview views.
 * Any IDataView resides within a page.
 */
export interface IDataView extends IHtmlElement {
    /**
     * Get the page where the dataview is displayed.
     */
    getPage(): FullPage;
    /**
     * This method can be called automatically when the browser window is
     * being resized, or it can be called as a result of a user action.
     */
    refresh(): void;
    /** Kind of view displayed */
    viewKind: ViewKind;
}
