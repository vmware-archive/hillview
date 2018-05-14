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
    /**
     * Each column can have a different display name.
     */
    private displayNameMap: Map<string, string>;
    private reverseDisplayNameMap: Map<string, string>;

    constructor(public readonly schema: Schema) {
        this.columnNames = [];
        this.columnMap = new Map<string, number>();
        for (let i = 0; i < this.schema.length; i++) {
            let colName = this.schema[i].name;
            this.columnNames.push(colName);
            this.columnMap.set(colName, i);
        }
        this.displayNameMap = new Map<string, string>();
        this.reverseDisplayNameMap = new Map<string, string>();
    }

    public copyDisplayNames(schema: SchemaClass): void {
        this.displayNameMap = new Map<string, string>(schema.displayNameMap);
        this.reverseDisplayNameMap = new Map<string, string>(schema.reverseDisplayNameMap);
    }

    public getRenameMap(): Map<string, string> {
        return this.displayNameMap;
    }

    /**
     * Get the display name of the specified column.
     */
    public displayName(name: string): string {
        if (this.displayNameMap.has(name))
            return this.displayNameMap.get(name);
        return name;
    }

    /**
     * Given a display name get the real column name.
     */
    public fromDisplayName(name: string): string {
        if (this.reverseDisplayNameMap.has(name))
            return this.reverseDisplayNameMap.get(name);
        console.assert(this.columnMap.has(name));
        return name;
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
        let colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    public findByDisplayName(colName: string): IColumnDescription {
        let original = this.fromDisplayName(colName);
        return this.find(original);
    }

    public filter(predicate: (IColumnDescription) => boolean): SchemaClass {
        let cols = this.schema.filter(predicate);
        let result = new SchemaClass(cols);
        result.copyDisplayNames(this);
        return result;
    }

    public append(cd: IColumnDescription): SchemaClass {
        let result = new SchemaClass(this.schema.concat(cd));
        result.copyDisplayNames(this);
        return result;
    }

    public concat(cds: IColumnDescription[]): SchemaClass {
        let result = new SchemaClass(this.schema.concat(cds));
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
        let result = new SchemaClass(this.schema);
        result.copyDisplayNames(this);
        return result;
    }
}