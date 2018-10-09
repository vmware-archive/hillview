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

export type RemoteObjectId = string;

export type Comparison = "==" | "!=" | "<" | ">" | "<=" | ">=";

export type ContentsKind = "Json" | "String" | "Integer" |
    "Double" | "Date" | "Interval";

/* We are not using an enum for ContentsKind because JSON deserialization does not
   return an enum from a string. */
export const allContentsKind: ContentsKind[] = ["Json", "String", "Integer", "Double", "Date", "Interval"];
export function asContentsKind(kind: string): ContentsKind {
    switch (kind) {
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
    colName: string;
    newColName: string;
    newKind: ContentsKind;
    columnIndex: number;
}

/// Same as FindSketch.Result
export interface FindResult {
    before: number;
    at: number;
    after: number;
    firstMatchingRow: any[];
}

export interface JdbcConnectionInformation {
    host: string;
    database: string;
    table: string;
    port: number;
    user: string;
    password: string;
    databaseKind: string;  // e.g. mysql; part of url for connection
    lazyLoading: boolean;
}

export type DataKinds = "csv" | "orc" | "parquet" | "json" | "hillviewlog" | "db" | "genericlog";

export interface FileSetDescription {
    fileKind: DataKinds;
    folder: string;
    fileNamePattern: string;
    schemaFile: string;
    headerRow?: boolean;
    cookie?: string;
    repeat: number;
    name: string;  // not used on the Java side
    logFormat: string; 
}

export interface HLogLog {
    distinctItemCount: number;
}

export interface CreateColumnInfo {
    jsFunction: string;
    schema: Schema;
    outputColumn: string;
    outputKind: ContentsKind;
    renameMap: string[];
}

export interface Heatmap {
    buckets: number[][];
    missingData: number;
    histogramMissingX: HistogramBase;
    histogramMissingY: HistogramBase;
    totalSize: number;
}

export interface Heatmap3D {
    buckets: number[][][];
    eitherMissing: number;
    totalPresent: number;
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
    Union, Intersection, Exclude, Replace,
}

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
}

export function kindIsNumeric(kind: ContentsKind): boolean {
    return kind === "Integer" || kind === "Double";
}

export function kindIsString(kind: ContentsKind): boolean {
    return kind === "String" || kind === "Json";
}

export type Schema = IColumnDescription[];

export interface RowSnapshot {
    count: number;
    values: any[];
}

export interface FileSizeSketchInfo {
    fileCount: number;
    totalSize: number;
}

export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

export interface HistogramBase {
    buckets: number[];
    missingData: number;
}

// This is actually a union of several java classes:
// StringBucketLeftBoundaries and DataRange, both sub-classes
// of BucketsInfo in Java.
export interface DataRange {
    presentCount: number;
    missingCount: number;
    // Only used for numeric data
    min?: number;
    max?: number;
    // Only used for string data
    leftBoundaries?: string[];
    maxBoundary?: string;
    allStringsKnown?: boolean;
}

export interface BasicColStats extends DataRange {
    momentCount: number;
    moments: number[];
}

export interface RangeArgs {
    cd: IColumnDescription;
    seed: number;
    stringsToSample: number;
}

// This is a union of DoubleRangeFilterDescription
// and StringRangeFilterDescription.
export interface FilterDescription {
    min: number;
    max: number;
    minString: string;
    maxString: string;
    cd: IColumnDescription;
    complement: boolean;
}

export interface HistogramArgs {
    cd: IColumnDescription;
    seed: number;
    samplingRate: number;
    bucketCount: number;  // sometimes superseded by leftBoundaries
    // only used when doing string histograms
    leftBoundaries?: string[];
    // only used when doing double histograms
    min?: number;
    max?: number;
}

export interface HeavyHittersFilterInfo {
    hittersId: string;
    schema: Schema;
}

export interface TopList {
    top: NextKList;
    heavyHittersId: RemoteObjectId;
}

export interface NextKArgs {
    toFind: string | null;
    order: RecordOrder;
    firstRow: any[] | null;
    rowsOnScreen: number;
}

export interface EigenVal {
    eigenValues: number [];
    explainedVar: number;
    totalVar: number;
    correlationMatrixTargetId: RemoteObjectId;
}

/**
 * The serialization of a NextKList Java object
 */
export interface NextKList {
    rowsScanned: number;
    startPosition: number;
    rows: RowSnapshot[];
}

export class RecordOrder {
    // Direct counterpart to Java class
    constructor(public sortOrientationList: ColumnSortOrientation[]) {}
    public length(): number { return this.sortOrientationList.length; }
    public get(i: number): ColumnSortOrientation { return this.sortOrientationList[i]; }

    // Find the index of a specific column; return -1 if columns is not in the sort order
    public find(col: string): number {
        for (let i = 0; i < this.length(); i++)
            if (this.sortOrientationList[i].columnDescription.name === col)
                return i;
        return -1;
    }

    public hide(col: string): void {
        const index = this.find(col);
        if (index === -1)
        // already hidden
            return;
        this.sortOrientationList.splice(index, 1);
    }

    public sortFirst(cso: ColumnSortOrientation): void {
        const index = this.find(cso.columnDescription.name);
        if (index !== -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }

    public addColumn(cso: ColumnSortOrientation): void {
        const index = this.find(cso.columnDescription.name);
        if (index !== -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.push(cso);
    }

    public addColumnIfNotVisible(cso: ColumnSortOrientation): void {
        const index = this.find(cso.columnDescription.name);
        if (index === -1)
            this.sortOrientationList.push(cso);
    }

    public clone(): RecordOrder {
        return new RecordOrder(this.sortOrientationList.slice(0));
    }

    // Returns a new object
    public invert(): RecordOrder {
        const result: ColumnSortOrientation[] = [];
        for (const cso of this.sortOrientationList) {
            result.push({
                isAscending: !cso.isAscending,
                columnDescription: cso.columnDescription,
            });
        }
        return new RecordOrder(result);
    }

    protected static coToString(cso: ColumnSortOrientation): string {
        return cso.columnDescription.name + " " + (cso.isAscending ? "up" : "down");
    }

    public toString(): string {
        let result = "";
        for (const soi of this.sortOrientationList)
            result += RecordOrder.coToString(soi);
        return result;
    }
}

/**
 * Describes the a filter that checks for (in)equality again a search string/pattern.
 */
export interface StringFilterDescription {
    /**
     * String/pattern that is being tested.
     */
    compareValue: string;
    /**
     * True if we want to allow substring matching.
     */
    asSubString: boolean;
    /**
     * True if we are looking to do regular expression matching.
     */
    asRegEx: boolean;
    /**
     * True if we want the search string to be case sensitive.
     */
    caseSensitive: boolean;
    /**
     * True if we are looking for anything that is not equal.
     */
    complement: boolean;
}

export interface StringRowFilterDescription {
    colName: string;
    stringFilterDescription: StringFilterDescription;
}

export interface ComparisonFilterDescription {
    /**
     * Column that is being filtered.
     */
    column: IColumnDescription;
    /**
     * Value that is being tested if the column is a string column.
     */
    stringValue: string;
    /**
     * Value that is being tested if the column is a numeric/date column.
     */
    doubleValue: number;
    comparison: Comparison;
}
