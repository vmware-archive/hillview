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

import {Comparison} from "./util";

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
    colName: string;
    newColName: string;
    newKind: ContentsKind;
    columnIndex: number;
}

/// Same as FindSketch.Result
export interface FindResult {
    count: number;
    firstRow: any[];
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

export type DataKinds = "csv" | "orc" | "parquet" | "json" | "hillviewlog" | "db";

export interface FileSetDescription {
    fileKind: DataKinds;
    folder: string;
    fileNamePattern: string;
    schemaFile: string;
    headerRow?: boolean;
    cookie?: string;
    repeat: number;
    name: string;  // not used on the Java side
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

export interface HeatMap {
    buckets: number[][];
    missingData: number;
    histogramMissingX: HistogramBase;
    histogramMissingY: HistogramBase;
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
    return kind === "Category" || kind === "String" || kind === "Json";
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

export class CategoricalValues {
    constructor(public columnName: string,
                public allNames?: string[]) {}
}

/**
 * All strings that can appear in a categorical column.
 * Serialization of the DistinctStrings Java class.
 */
export interface IDistinctStrings {
    uniqueStrings: string[];
    // This may be true if there are too many distinct strings in a column.
    truncated: boolean;
}

export interface HistogramBase {
    buckets: number[];
    missingData: number;
    outOfRange: number;
}

export interface NumericColumnStatistics {
    momentCount: number;
    min: number;
    max: number;
    moments: number[];
    presentCount: number;
    missingCount: number;
}

export interface StringColumnStatistics {
    min: string;
    max: string;
    presentCount: number;
    missingCount: number;
}

export interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    moments: number[];
    presentCount: number;
    missingCount: number;
}

export interface ColumnAndRange {
    min: number;
    max: number;
    onStrings: boolean;
    columnName: string;
    bucketBoundaries: string[];
}

export interface HistogramArgs {
    column: ColumnAndRange;
    cdfBucketCount: number;
    bucketCount: number;
    samplingRate: number;
    cdfSamplingRate: number;
    seed: number;
}

export interface Histogram2DArgs {
    first: ColumnAndRange;
    second: ColumnAndRange;
    xBucketCount: number;
    yBucketCount: number;
    samplingRate: number;
    seed: number;
    cdfBucketCount: number;
    cdfSamplingRate: number;
}

export interface Histogram3DArgs {
    first: ColumnAndRange;
    second: ColumnAndRange;
    third: ColumnAndRange;
    xBucketCount: number;
    yBucketCount: number;
    zBucketCount: number;
    samplingRate: number;
    seed: number;
}

export interface FilterDescription {
    min: number;
    max: number;
    columnName: string;
    kind: ContentsKind;
    complement: boolean;
    bucketBoundaries: string[];
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

    public sortFirst(cso: ColumnSortOrientation) {
        const index = this.find(cso.columnDescription.name);
        if (index !== -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }

    public addColumn(cso: ColumnSortOrientation) {
        const index = this.find(cso.columnDescription.name);
        if (index !== -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.push(cso);
    }

    public addColumnIfNotVisible(cso: ColumnSortOrientation) {
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
 * Describes the a filter that checks for (in)equality.
 */
export interface EqualityFilterDescription {
    /**
     * Column that is being filtered.
     */
    column: string;
    /**
     * Column that is being filtered.
     */
    compareValue: string;
    /**
     * True if we are looking for anything that is not equal.
     */
    complement: boolean;
    /**
     * True if we are looking to do regular expression matching.
     */
    asRegEx: boolean;
}

export interface ComparisonFilterDescription {
    /**
     * Column that is being filtered.
     */
    column: string;
    /**
     * Column that is being filtered.
     */
    compareValue: string;
    comparison: Comparison;
}
