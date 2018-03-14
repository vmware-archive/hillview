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

import {IColumnDescription, EqualityFilterDescription} from "../javaBridge";
import {Dialog, FieldKind} from "../ui/dialog"
import {Converters} from "../util";

export class EqualityFilterDialog extends Dialog {
    constructor(private columnDescription: IColumnDescription) {
        super("Filter on " + columnDescription.name, "Eliminates data from a column according to its value.");
        this.addTextField("query", "Find:", FieldKind.String, null, "Value to search");
        this.addBooleanField("asRegEx", "Interpret as Regular Expression", false, "Select "
            + "checkbox to interpret the search query as a regular expression");
        this.addBooleanField("complement", "Exclude matches", false, "Select checkbox to "
            + "filter out all matches");
    }

    public getFilter(): EqualityFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        if (this.columnDescription.kind == "Date") {
            let date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        let asRegEx = this.getBooleanValue("asRegEx");
        let complement = this.getBooleanValue("complement");
        return {
            column: this.columnDescription.name,
            compareValue: textQuery,
            complement: complement,
            asRegEx: asRegEx
        };
    }
}