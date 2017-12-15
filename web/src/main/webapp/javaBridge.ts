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

// This file contains data structures that are used to serialize information between
// TypeScript and Java.  These must be changed carefully, and usually in both parts, or
// subtle bugs may happen.  Most often these classes have the same name in Java and TypeScript.

import {Seed} from "./util";

export type RemoteObjectId = string;

export type ContentsKind = "Category" | "Json" | "String" | "Integer" |
    "Double" | "Date" | "Interval";

/* We are not using an enum for ContentsKind because JSON deserialization does not
   return an enum from a string. */
export const allContentsKind: ContentsKind[] = ["Category", "Json", "String", "Integer", "Double", "Date", "Interval"];
export function asContentsKind(kind: string): ContentsKind {
    switch (kind) {
        case "Category": {
            return "Category";
        }
        case "Json": {
            return "Json";
        }
        case "String": {
            return "String";
        }
        case "Integer": {
            return "Integer";
        }
        case "Double": {
            return "Double";
        }
        case "Date": {
            return "Date";
        }
        case "Interval": {
            return "Interval";
        }
        default: {
            throw new TypeError(`String ${kind} is not a kind.`);
        }
    }
}

export interface TableSummary {
    schema: Schema;
    rowCount: number;
}

export interface ConvertColumnInfo {
    colName: string,
    newColName: string,
    newKind: ContentsKind,
    columnIndex: number
}

export interface JdbcConnectionInformation {
    host: string;
    database: string;
    table: string;
    port: number;
    user: string;
    password: string;
    databaseKind: string;  // e.g. mysql; part of url for connection
}

export interface FileSetDescription {
    folder: string;
    fileNamePattern: string;
    schemaFile: string;
    headerRow?: boolean;
    cookie?: string;
}

export interface HLogLog {
    distinctItemCount: number
}

export interface CreateColumnInfo {
    jsFunction: string;
    schema: Schema;
    outputColumn: string;
    outputKind: ContentsKind;
}

export class HeatMap {
    buckets: number[][];
    missingData: number;
    histogramMissingX: Histogram;
    histogramMissingY: Histogram;
    totalSize: number;
}

/**
 * Corresponds to the Java class ControlMessage.Status.
 */
export interface Status {
    hostname: string;
    result: string;
    exception: string;
}

export enum CombineOperators {
    Union, Intersection, Exclude, Replace
}

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
}

export class ColumnDescription implements IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;

    constructor(v : IColumnDescription) {
        this.kind = v.kind;
        this.name = v.name;
        this.allowMissing = v.allowMissing;
    }
}

export function isNumeric(kind: ContentsKind): boolean {
    return kind == "Integer" || kind == "Double";
}

export interface Schema {
    [index: number] : IColumnDescription;
    length: number;
}

/**
 * Serialization of a Java RowSnapshot.
 */
export interface RowView {
    count: number;
    values: any[];
}

export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

export class RangeInfo {
    seed: number;
    constructor(public columnName: string,
                // The following is only used for categorical columns
                public allNames?: string[]) {
        this.seed = Seed.instance.get();
    }
}

export interface Histogram {
    buckets: number[]
    missingData: number;
    outOfRange: number;
}

export interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    moments: Array<number>;
    presentCount: number;
    missingCount: number;
}

export interface ColumnAndRange {
    min: number;
    max: number;
    columnName: string;
    bucketBoundaries: string[];
}

export class HistogramArgs {
    column: ColumnAndRange;
    cdfBucketCount: number;
    bucketCount: number;
    samplingRate: number;
    cdfSamplingRate: number;
    seed: number;
}

export class Histogram2DArgs {
    first: ColumnAndRange;
    second: ColumnAndRange;
    xBucketCount: number;
    yBucketCount: number;
    samplingRate: number;
    seed:         number;
    cdfBucketCount: number;
    cdfSamplingRate: number;
}

export class Histogram3DArgs {
    first: ColumnAndRange;
    second: ColumnAndRange;
    third: ColumnAndRange;
    xBucketCount: number;
    yBucketCount: number;
    zBucketCount: number;
    samplingRate: number;
    seed:         number;
}

export interface FilterDescription {
    min: number;
    max: number;
    columnName: string;
    kind: ContentsKind,
    complement: boolean;
    bucketBoundaries: string[];
}


export interface TopList {
    top: NextKList;
    heavyHittersId: string;
}

/**
 * The serialization of a NextKList Java object
 */
export class NextKList {
    public schema: Schema;
    // Total number of rows in the complete table
    public rowCount: number;
    public startPosition: number;
    public rows: RowView[];
}

export class RecordOrder {
    // Direct counterpart to Java class
    constructor(public sortOrientationList: Array<ColumnSortOrientation>) {}
    public length(): number { return this.sortOrientationList.length; }
    public get(i: number): ColumnSortOrientation { return this.sortOrientationList[i]; }

    // Find the index of a specific column; return -1 if columns is not in the sort order
    public find(col: string): number {
        for (let i = 0; i < this.length(); i++)
            if (this.sortOrientationList[i].columnDescription.name == col)
                return i;
        return -1;
    }
    public hide(col: string): void {
        let index = this.find(col);
        if (index == -1)
        // already hidden
            return;
        this.sortOrientationList.splice(index, 1);
    }
    public sortFirst(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }
    public show(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.push(cso);
    }
    public showIfNotVisible(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index == -1)
            this.sortOrientationList.push(cso);
    }
    public clone(): RecordOrder {
        return new RecordOrder(this.sortOrientationList.slice(0));
    }
    // Returns a new object
    public invert(): RecordOrder {
        let result = new Array<ColumnSortOrientation>(this.sortOrientationList.length);
        for (let i in this.sortOrientationList) {
            let cso = this.sortOrientationList[i];
            result[i] = {
                isAscending: !cso.isAscending,
                columnDescription: cso.columnDescription
            };
        }
        return new RecordOrder(result);
    }

    protected static coToString(cso: ColumnSortOrientation): string {
        return cso.columnDescription.name + " " + (cso.isAscending ? "up" : "down");
    }
    public toString(): string {
        let result = "";
        for (let i = 0; i < this.sortOrientationList.length; i++)
            result += RecordOrder.coToString(this.sortOrientationList[i]);
        return result;
    }
}
