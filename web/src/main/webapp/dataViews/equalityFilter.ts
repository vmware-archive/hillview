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

import {ColumnDescription} from "../javaBridge";
import {Dialog, FieldKind} from "../ui/dialog"
import {Converters} from "../util";

/**
 * Describes the a filter that checks for (in)equality.
 */
export class EqualityFilterDescription {
    /**
     * Column that is being filtered.
     */
    column: string;
    /**
     * Value to look for (represented as a string).
     */
    compareValue: string;
    /**
     * True if we are looking for anything that is not equal.
     */
    complement: boolean;
}

export class EqualityFilterDialog extends Dialog {
    constructor(private columnDescription: ColumnDescription) {
        super("Filter on " + columnDescription.name, "Eliminates data from a column according to its value.");
        this.addTextField("query", "Find:", FieldKind.String, null, "Value to search");
        this.addSelectField("complement", "Check for:", ["Equality", "Inequality"], null,
            "For 'equality' the search will keep all values that are equal to the one chosen," +
        "For 'inequality' the search will keep all values that are different from the one chosen.");
    }

    public getFilter(): EqualityFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        let complement = this.getFieldValue("complement") == "Inequality";
        if (this.columnDescription.kind == "Date") {
            let date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        return {
            column: this.columnDescription.name,
            compareValue: textQuery,
            complement: complement,
        };
    }
}