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

import {ColumnDescription} from "./tableData";
import {Dialog} from "./dialog"
import {Converters} from "./util";

// Class explaining the search we want to perform
export class EqualityFilterDescription {
    columnDescription: ColumnDescription;
    compareValue: string;
    complement: boolean;
}

// Dialog that has fields for making an EqualityFilterDescription.
export class EqualityFilterDialog extends Dialog {
    constructor(private columnDescription: ColumnDescription) {
        super("Filter");
        this.addTextField("query", "Find:", columnDescription.kind);
        this.addSelectField("complement", "Check for:", ["Equality", "Inequality"]);
    }

    public getFilter(): EqualityFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        let complement = this.getFieldValue("complement") == "Inequality";
        if (this.columnDescription.kind == "Date") {
            let date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        return {
            columnDescription: this.columnDescription,
            compareValue: textQuery,
            complement: complement,
        };
    }
}