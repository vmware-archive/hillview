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
import {cloneArray, mapToArray, Serializable} from "./util";

export interface SchemaClassSerialization {
    schema: Schema;
    displayNameMap: string[];
}

/**
 * A wrapper for a name of a column that is being displayed.
 * We put this in a class to make it easier for the compiler to catch misuses of column names.
 */
export class DisplayName {
    public constructor(public displayName: string) {
        console.assert(displayName !== null);
    }
    public toString(): string {
        return this.displayName;
    }
    public equals(other: DisplayName): boolean {
        return this.displayName === other.displayName;
    }
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
    private displayNameMap: Map<string, DisplayName>;
    /**
     * Maps the display name (the string part) to the real name.
     * Reverse of the displayNameMap.
     */
    private reverseDisplayNameMap: Map<string, string>;

    private initialize(): void {
        this.columnNames = [];
        this.columnMap = new Map<string, number>();
        for (let i = 0; i < this.schema.length; i++) {
            const colName = this.schema[i].name;
            this.columnNames.push(colName);
            this.columnMap.set(colName, i);
        }
        this.displayNameMap = new Map<string, DisplayName>();
        this.reverseDisplayNameMap = new Map<string, string>();
    }

    constructor(public schema: Schema) {
        this.initialize();
    }

    public copyDisplayNames(schema: SchemaClass): void {
        this.displayNameMap = new Map<string, DisplayName>();
        for (const col of this.columnNames) {
            if (schema.displayNameMap.has(col)) {
                const displayName = schema.displayNameMap.get(col);
                this.displayNameMap.set(col, displayName);
                this.reverseDisplayNameMap.set(displayName.displayName, col);
            }
        }
    }

    /**
     * Returns an array encoding the column renaming: even elements are original names,
     * odd elements are new names.
     */
    public getRenameVector(): string[] {
        const result: string[] = [];
        this.displayNameMap.forEach((v, k) => { result.push(k); result.push(v.displayName); });
        return result;
    }

    public serialize(): SchemaClassSerialization {
        return {
            schema: this.schema,
            displayNameMap: mapToArray(this.displayNameMap),
        };
    }

    public deserialize(data: SchemaClassSerialization): SchemaClass | null {
        if (data == null)
            return null;
        this.schema = data.schema;
        const dn: string[] = data.displayNameMap;

        if (this.schema == null || dn == null)
            return null;
        this.initialize();
        if (dn.length % 2 !== 0)
            return null;
        for (let i = 0; i < dn.length; i += 2) {
            const success = this.changeDisplayName(new DisplayName(dn[i]), dn[i + 1]);
            if (!success)
                return null;
        }
        return this;
    }

    public allDisplayNames(): DisplayName[] {
        return this.columnNames.map((c) => this.displayName(c)!);
    }

    /**
     * All display names except the ones for the specified columns.
     * @param names  Names to exclude.
     */
    public displayNamesExcluding(names: string[]): DisplayName[] {
        return this.columnNames
            .filter((n) => names.indexOf(n) < 0)
            .map((c) => this.displayName(c)!);
    }

    /**
     * Get the display name of the specified column.
     */
    public displayName(name: string | null): DisplayName | null {
        if (name == null)
            return null;
        if (this.displayNameMap.has(name))
            return this.displayNameMap.get(name);
        return new DisplayName(name);
    }

    /**
     * Given a display name get the real column name.
     */
    public fromDisplayName(displayName: DisplayName | null): string | null {
        if (displayName == null)
            return null;
        if (this.reverseDisplayNameMap.has(displayName.displayName))
            return this.reverseDisplayNameMap.get(displayName.displayName);
        console.assert(this.columnMap.has(displayName.displayName));
        return displayName.displayName;
    }

    /**
     * Change the display name of a column.  Mutates the current schema!
     * Display names have to be unique within a schema.
     * @returns {boolean}  True if the new name is acceptable.
     */
    public changeDisplayName(name: DisplayName, to: string): boolean {
        if (this.reverseDisplayNameMap.has(to))
            return false;
        if (this.columnMap.has(to) && !this.displayNameMap.has(to))
            return false;
        if (this.displayNameMap.has(name.displayName))
            this.reverseDisplayNameMap.delete(this.displayNameMap.get(name.displayName).displayName);
        this.displayNameMap.set(name.displayName, new DisplayName(to));
        this.reverseDisplayNameMap.set(to, name.displayName);
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

    public find(colName: string | null): IColumnDescription | null {
        if (colName == null)
            return null;
        const colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    public findByDisplayName(displayName: DisplayName | null): IColumnDescription | null {
        const original = this.fromDisplayName(displayName);
        return this.find(original);
    }

    public filter(predicate: (c: IColumnDescription) => boolean): SchemaClass {
        const cols = this.schema.filter(predicate);
        const result = new SchemaClass(cols);
        result.copyDisplayNames(this);
        return result;
    }

    public insert(cd: IColumnDescription, index: number): SchemaClass {
        if (index < 0 || index >= this.length)
            return this.append(cd);
        const copy = cloneArray(this.schema);
        copy.splice(index, 0, cd);
        const result = new SchemaClass(copy);
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
