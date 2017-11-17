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

import {InitialObject} from "./initialObject";
import {FullPage} from "./ui/fullPage";
import {createLoadMenu} from "./loadMenu";

/**
 * This file exports all TypeScript classes which can be used directly
 * in index.html.
 */

/**
 * This is a workaround webpack limitations: we export all symbols we need
 * by making them fields of the window object.
 */
let public_symbols = {
    createLoadMenu,
    FullPage   // used for resizing
};

window["hillview"] = public_symbols;
