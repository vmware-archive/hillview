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

// Used for operations between multiple objects: the selected object
// is a RemoteTableObjectView which can be combined with another one.
import {MenuItem, SubMenu} from "./ui/menu";
import {EnumIterators} from "./util";
import {CombineOperators} from "./javaBridge";
import {RemoteTableObject, RemoteTableObjectView} from "./tableTarget";
import {ErrorReporter} from "./ui/errReporter";

export class SelectedObject {
    private selected: RemoteTableObjectView = null;
    private pageId: number;  // page containing the object

    select(object: RemoteTableObjectView, pageId: number) {
        this.selected = object;
        this.pageId = pageId;
    }

    /**
     * Check if the selected object can be combined with the specified one,
     * and if so return it.  Otherwise write an error message and return null.
     */
    getSelected(compatible: RemoteTableObject, reporter: ErrorReporter): RemoteTableObjectView {
        if (this.selected == null) {
            reporter.reportError("No object is currently selected");
            return null;
        }
        if (this.selected.originalTableId != compatible.originalTableId) {
            reporter.reportError("These two views cannot be combined because they are based on different data sets.");
            return null;
        }
        return this.selected;
    }

    getPage(): number {
        return this.pageId;
    }

    static instance: SelectedObject = new SelectedObject();
}

export function combineMenu(ro: RemoteTableObjectView, pageId: number): SubMenu {
    let combineMenu: MenuItem[] = [];
    combineMenu.push({
        text: "Select current",
        action: () => { SelectedObject.instance.select(ro, pageId); },
        help: "Save the current view; later it can be combined with another view, using one of the operations below."
    });
    combineMenu.push({text: "---", action: null, help: null});
    EnumIterators.getNamesAndValues(CombineOperators)
        .forEach(c => combineMenu.push({
            text: c.name,
            action: () => { ro.combine(c.value); },
            help: "Combine the rows in the two views using the " + c.value + " operation"
        }));
    return new SubMenu(combineMenu);
}

