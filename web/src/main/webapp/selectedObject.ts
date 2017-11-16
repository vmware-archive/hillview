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
// is a RemoteObject which can be combined with another one.
import {RemoteObject} from "./rpc";
import {SubMenu} from "./ui/menu";
import {EnumIterators} from "./util";
import {CombineOperators} from "./javaBridge";

export class SelectedObject {
    private selected: RemoteObject = null;
    private pageId: number;  // page containing the object

    select(object: RemoteObject, pageId: number) {
        this.selected = object;
        this.pageId = pageId;
    }

    getSelected(): RemoteObject {
        return this.selected;
    }

    getPage(): number {
        return this.pageId;
    }

    static current: SelectedObject = new SelectedObject();
}

export function combineMenu(ro: RemoteObject, pageId: number): SubMenu {
    let combineMenu = [];
    combineMenu.push({
        text: "Select current",
        action: () => { SelectedObject.current.select(ro, pageId); }});
    combineMenu.push({text: "---", action: null});
    EnumIterators.getNamesAndValues(CombineOperators)
        .forEach(c => combineMenu.push({
            text: c.name,
            action: () => { ro.combine(c.value); } }));
    return new SubMenu(combineMenu);
}

