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
import {assert, cloneArray, Serializable} from "./util";

/**
 * A SchemaClass is a class containing a Schema and some indexes and methods
 * for fast access.
 */
export class SchemaClass implements Serializable<SchemaClass> {
    private columnMap: Map<string, number>;

    // Return 'true' if it succeeds.
    private initialize(): boolean {
        this.columnMap = new Map<string, number>();
        for (let i = 0; i < this.schema.length; i++) {
            const colName = this.schema[i].name;
            if (this.columnMap.has(colName))
                return false;
            this.columnMap.set(colName, i);
        }
        return true;
    }

    constructor(public schema: Schema) {
        assert(this.initialize());
    }

    public serialize(): Schema {
        return this.schema;
    }

    public deserialize(data: Schema): SchemaClass | null {
        return new SchemaClass(data);
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

    /**
     * Rename the specified column.  Return null on failure.
     */
    public renameColumn(from: string, to: string): SchemaClass | null {
        if (this.find(to) != null) {
            return null;
        }
        const s = this.schema.map(c => c.name == from ? { name: to, kind: c.kind } : c);
        return new SchemaClass(s);
    }

    public columnIndex(colName: string): number {
        if (!this.columnMap.has(colName))
            return -1;
        return this.columnMap.get(colName);
    }

    public find(colName: string | null): IColumnDescription | null {
        if (colName == null)
            return null;
        const colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    public filter(predicate: (c: IColumnDescription) => boolean): SchemaClass {
        const cols = this.schema.filter(predicate);
        return new SchemaClass(cols);
    }

    public insert(cd: IColumnDescription, index: number): SchemaClass {
        if (index < 0 || index >= this.length)
            return this.append(cd);
        const copy = cloneArray(this.schema);
        copy.splice(index, 0, cd);
        return new SchemaClass(copy);
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

    public clone(): SchemaClass {
        return this.concat([]);
    }

    public allColumnNames(): string[] {
        return this.schema.map(d => d.name);
    }

    public namesExcluding(names: string[]): string[] {
        return this.schema.map(c => c.name)
            .filter((n) => names.indexOf(n) < 0);
    }

    public getDescriptions(columns: string[]): (IColumnDescription | null)[] {
        const cds: (IColumnDescription | null)[] = [];
        columns.forEach((v) => {
            const colDesc = this.find(v);
            cds.push(colDesc);
        });
        return cds;
    }

    public getCheckedDescriptions(columns: string[]): (IColumnDescription)[] {
        const cds: IColumnDescription[] = [];
        columns.forEach((v) => {
            const colDesc = this.find(v);
            cds.push(colDesc!);
        });
        return cds;
    }
}
