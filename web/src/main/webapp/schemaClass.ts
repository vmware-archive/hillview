/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import {IColumnDescription, Schema} from "./javaBridge";

/**
 * A SchemaClass is a class containing a Schema and some indexes and methods
 * for fast access.
 */
export class SchemaClass {
    public readonly columnNames: string[];
    private readonly columnMap: Map<string, number>;

    constructor(public readonly schema: Schema) {
        this.columnNames = [];
        this.columnMap = new Map<string, number>();
        for (let i = 0; i < this.schema.length; i++) {
            let colName = this.schema[i].name;
            this.columnNames.push(colName);
            this.columnMap.set(colName, i);
        }
    }

    public uniqueColumnName(prefix: string): string {
        let name = prefix;
        let i = 0;
        while (this.columnMap.has(name)) {
            name = prefix + `_${i}`;
            i++;
        }
        return name;
    }

    public columnIndex(colName: string): number {
        if (!this.columnMap.has(colName))
            return -1;
        return this.columnMap.get(colName);
    }

    public find(colName: string): IColumnDescription {
        if (colName == null)
            return null;
        let colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    public filter(predicate): SchemaClass {
        let cols = this.schema.filter(predicate);
        return new SchemaClass(cols);
    }

    public append(cd: IColumnDescription): SchemaClass {
        return new SchemaClass(this.schema.concat(cd));
    }

    public concat(cds: IColumnDescription[]): SchemaClass {
        return new SchemaClass(this.schema.concat(cds));
    }

    public get length(): number {
        return this.schema.length;
    }

    public get(index: number): IColumnDescription {
        return this.schema[index];
    }
}