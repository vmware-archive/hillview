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
import {mapToArray, Serializable} from "./util";

export interface SchemaClassSerialization {
    schema: Schema;
    displayNameMap: string[];
}

/**
 * A SchemaClass is a class containing a Schema and some indexes and methods
 * for fast access.
 */
export class SchemaClass implements Serializable<SchemaClass> {
    public columnNames: string[];
    private columnMap: Map<string, number>;
    /**
     * Each column can have a different display name.
     */
    private displayNameMap: Map<string, string>;
    private reverseDisplayNameMap: Map<string, string>;

    private initialize(): void {
        this.columnNames = [];
        this.columnMap = new Map<string, number>();
        for (let i = 0; i < this.schema.length; i++) {
            const colName = this.schema[i].name;
            this.columnNames.push(colName);
            this.columnMap.set(colName, i);
        }
        this.displayNameMap = new Map<string, string>();
        this.reverseDisplayNameMap = new Map<string, string>();
    }

    constructor(public schema: Schema) {
        this.initialize();
    }

    public copyDisplayNames(schema: SchemaClass): void {
        this.displayNameMap = new Map<string, string>();
        this.reverseDisplayNameMap = new Map<string, string>();
        for (const col of this.columnNames) {
            if (schema.displayNameMap.has(col)) {
                const displayName = schema.displayNameMap.get(col);
                this.displayNameMap.set(col, displayName);
                this.reverseDisplayNameMap.set(displayName, col);
            }
        }
    }

    public getRenameMap(): Map<string, string> {
        return this.displayNameMap;
    }

    public serialize(): SchemaClassSerialization {
        return {
            schema: this.schema,
            displayNameMap: mapToArray(this.displayNameMap),
        };
    }

    public deserialize(data: SchemaClassSerialization): SchemaClass {
        this.schema = data.schema;
        const dn: string[] = data.displayNameMap;

        if (this.schema == null || dn == null)
            return null;
        this.initialize();
        if (dn.length % 2 !== 0)
            return null;
        for (let i = 0; i < dn.length; i += 2) {
            const success = this.changeDisplayName(dn[i], dn[i + 1]);
            if (!success)
                return null;
        }
        return this;
    }

    public allDisplayNames(): string[] {
        return this.columnNames.map((c) => this.displayName(c));
    }

    /**
     * Get the display name of the specified column.
     */
    public displayName(name: string | null): string | null {
        if (name == null)
            return null;
        if (this.displayNameMap.has(name))
            return this.displayNameMap.get(name);
        return name;
    }

    /**
     * Given a display name get the real column name.
     */
    public fromDisplayName(displayName: string | null): string | null {
        if (displayName == null)
            return null;
        if (this.reverseDisplayNameMap.has(displayName))
            return this.reverseDisplayNameMap.get(displayName);
        console.assert(this.columnMap.has(displayName));
        return displayName;
    }

    /**
     * Change the display name of a column.
     * Display names have to be unique within a schema.
     * @returns {boolean}  True if the new name is acceptable.
     */
    public changeDisplayName(name: string, to: string): boolean {
        if (this.reverseDisplayNameMap.has(to))
            return false;
        if (this.columnMap.has(to) && !this.displayNameMap.has(to))
            return false;
        if (this.displayNameMap.has(name))
            this.reverseDisplayNameMap.delete(this.displayNameMap.get(name));
        this.displayNameMap.set(name, to);
        this.reverseDisplayNameMap.set(to, name);
        return true;
    }

    public uniqueColumnName(prefix: string): string {
        let name = prefix;
        let i = 0;
        while (this.columnMap.has(name) || this.reverseDisplayNameMap.has(name)) {
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
        const colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    public findByDisplayName(displayName: string): IColumnDescription {
        const original = this.fromDisplayName(displayName);
        return this.find(original);
    }

    public filter(predicate: (IColumnDescription) => boolean): SchemaClass {
        const cols = this.schema.filter(predicate);
        const result = new SchemaClass(cols);
        result.copyDisplayNames(this);
        return result;
    }

    public append(cd: IColumnDescription): SchemaClass {
        const result = new SchemaClass(this.schema.concat(cd));
        result.copyDisplayNames(this);
        return result;
    }

    public concat(cds: IColumnDescription[]): SchemaClass {
        const result = new SchemaClass(this.schema.concat(cds));
        result.copyDisplayNames(this);
        return result;
    }

    public get length(): number {
        return this.schema.length;
    }

    public get(index: number): IColumnDescription {
        return this.schema[index];
    }

    public clone(): SchemaClass {
        const result = new SchemaClass(this.schema);
        result.copyDisplayNames(this);
        return result;
    }

    public getDescriptions(columns: string[]): IColumnDescription[] {
        const cds: IColumnDescription[] = [];
        columns.forEach((v) => {
            const colDesc = this.find(v);
            cds.push(colDesc);
        });
        return cds;
    }
}
