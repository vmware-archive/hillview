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
    "Double" | "Date" | "Duration" | "Interval" | "Time" | "LocalDate" ;
/* We are not using an enum for ContentsKind because JSON deserialization does not
   return an enum from a string. */
export const allContentsKind: ContentsKind[] =
    ["Json", "String", "Integer", "Double", "Date", "Duration", "Interval", "Time", "LocalDate"];
export function asContentsKind(kind: string): ContentsKind {
    switch (kind) {
        case "Json":
            return "Json";
        case "String":
            return "String";
        case "Integer":
            return "Integer";
        case "Double":
            return "Double";
        case "Date":
            return "Date";
        case "Time":
            return "Time";
        case "Duration":
            return "Duration";
        case "Interval":
            return "Interval";
        case "LocalDate":
            return "LocalDate";
        default:
            throw new TypeError(`String ${kind} is not a kind.`);
    }
}

/**
 * This must match the data in LogFiles.java
 */
export class GenericLogs {
    public static readonly timestampColumnName = "Timestamp";
    public static readonly parseErrorColumn = "ParsingErrors";
    public static readonly hostColumn = "Host";
    public static readonly directoryColumn = "Directory";
    public static readonly filenameColumn = "Filename";
    public static readonly lineNumberColumn = "Line";

    public static readonly logFileFixedSchema: Schema = [
        { name: GenericLogs.directoryColumn, kind: "String" },
        { name: GenericLogs.filenameColumn, kind: "String" },
        { name: GenericLogs.lineNumberColumn, kind: "Integer" },
        { name: GenericLogs.timestampColumnName, kind: "Date" },
        { name: GenericLogs.hostColumn, kind: "String" },
        { name: GenericLogs.parseErrorColumn, kind: "String" }
    ];
}

// Describes the configuration of the UI for a specific installation of Hillview
// The default values should all be 'false'
export interface UIConfig {
    enableSaveAs?: boolean;  // save as menu enabled
    localDbMenu?: boolean;   // show the local db connection
    showTestMenu?: boolean;  // the test menu is shown
    enableManagement?: boolean;  // management menu enabled
    privateIsCsv?: boolean;    // the private dataset is in csv form
    hideSuggestions?: boolean; // do not display the suggestions
}

export interface ColumnQuantization {
    globalMax: number | string;
    // Only used for numeric columns
    globalMin: number | string;
    granularity: number | null;
    // Only used for string columns
    leftBoundaries: string[] | null;
}

export interface PrivacySchema {
    quantization: { quantization: { [colName: string]: ColumnQuantization } };
    epsilons: { [colNames: string]: number };
    // map a column count (encoded as a string, to suit JSON label requirements) to an epsilon
    defaultEpsilons: { [count: string]: number };
    defaultEpsilon: number;
}

// This class is used to package a string argument that should be sent
// unchanged through a remote all.
export class JsonString {
    constructor(private value: string) {}
    public toJSON(): string { return this.value; }
}

export interface TableSummary {
    schema: Schema;
    rowCount: number;
    rowCountConfidence: number | null;  // only present for private data
    metadata: PrivacySchema | null;     // only present for private data
}

export interface ConvertColumnInfo {
    colName: string;
    newColName: string;
    newKind: ContentsKind;
    columnIndex: number;
}

export type RowValue = number | string | number[];

/// Same as FindSketch.Result
export interface FindResult {
    before: number;
    at: number;
    after: number;
    firstMatchingRow: RowValue[];
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

export interface CassandraConnectionInfo extends JdbcConnectionInformation {
    jmxPort: number;
    cassandraRootDir: string;
}

export type DataKinds = "csv" | "orc" | "parquet" | "json" | "hillviewlog" | "db" | "genericlog" | "sstable";

export interface FileSetDescription {
    fileKind: DataKinds;
    fileNamePattern: string;
    schemaFile: string;
    headerRow?: boolean;
    cookie?: string;
    repeat: number;
    name: string;  // not used on the Java side
    logFormat: string;
    startTime: number | null;
    endTime: number | null;
}

export interface CountWithConfidence {
    count: number;
    confidence: number;
}

export interface CreateColumnJSMapInfo {
    jsFunction: string;
    schema: Schema;
    outputColumn: string;
    outputKind: ContentsKind;
    renameMap: string[];
}

export interface ExtractValueFromKeyMapInfo {
    key: string;
    inputColumn: IColumnDescription;
    outputColumn: string;
    outputIndex: number;
}

export type AggregateKind = "Sum" | "Count" | "Min" | "Max" | "Average";
export const allAggregateKind: AggregateKind[] = ["Sum", "Count", "Min", "Max", "Average"];

export interface AggregateDescription {
    agkind: AggregateKind;
    cd: IColumnDescription;
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

export interface RowData {
    count: number;
    values: RowValue[];
}

export interface FileSizeSketchInfo {
    fileCount: number;
    totalSize: number;
}

export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

// This is actually a union of two java classes
// StringQuantiles and DataRange, both sub-classes
// of BucketsInfo in Java.
export interface BucketsInfo {
    presentCount: number;
    missingCount: number;
    // Only used for numeric data
    min?: number;
    max?: number;
    // Only used for string data
    stringQuantiles?: string[];
    maxBoundary?: string;
    allStringsKnown?: boolean;
}

export interface BasicColStats {
    presentCount: number;
    missingCount: number;
    // Only used for numeric data
    min?: number;
    max?: number;
    moments: number[];
    // Only used for string data
    minString?: string;
    maxString?: string;
}

export interface RangeFilterDescription {
    min: number;
    max: number;
    minString: string;
    maxString: string;
    cd: IColumnDescription;
    includeMissing: boolean;
}

export interface RangeFilterArrayDescription {
    filters: RangeFilterDescription[];
    complement: boolean;
}

export interface JSFilterInfo {
    jsCode: string;
    schema: Schema;
    renameMap: string[];
}

export interface RowFilterDescription {
    order: RecordOrder;
    data: RowValue[];
    comparison: string;
}

export interface SampleSet {
    min: number;
    max: number;
    count: number;
    missing: number;
    samples: number[];
}

export interface CreateIntervalColumnMapInfo {
    startColName: string;
    endColName: string;
    columnIndex: number;
    newColName: string;
}

export interface CompareDatasetsInfo {
    names: string[];
    otherIds: string[];
    outputName: string;
}

export interface Groups<R> {
    perBucket: R[];
    perMissing: R;
}

export interface HistogramInfo {
    cd: IColumnDescription;
    bucketCount: number;  // sometimes superseded by leftBoundaries
    // only used when doing string histograms
    leftBoundaries?: string[];
    // only used when doing double histograms
    min?: number;
    max?: number;
}

export interface HistogramRequestInfo {
    histos: HistogramInfo[];
    samplingRate: number;
    seed: number;
}

export interface HeatmapRequestInfo extends HistogramRequestInfo {
    schema: Schema;
}

export interface QuantilesVectorInfo extends HistogramInfo {
    seed: number;
    quantileCount: number,
    quantilesColumn: string;
}

export interface QuantilesMatrixInfo extends QuantilesVectorInfo {
    groupColumn: HistogramInfo;
}

export interface HeavyHittersFilterInfo {
    hittersId: string;
    schema: Schema;
}

export interface TopList {
    top: NextKList;
    heavyHittersId: RemoteObjectId;
}

export interface ContainsArgs {
    order: RecordOrder;
    row: RowValue[];
}

export interface NextKArgs {
    toFind: string | null;
    order: RecordOrder;
    firstRow: RowValue[] | null;
    rowsOnScreen: number;
    columnsNoValue: string[] | null;
    aggregates: AggregateDescription[] | null;
}

export interface EigenVal {
    eigenValues: number [];
    explainedVar: number;
    totalVar: number;
    correlationMatrixTargetId: RemoteObjectId;
}

export interface NextKList {
    rowsScanned: number;
    startPosition: number;
    rows: RowData[];
    aggregates: number[][] | null;
}

export class RecordOrder {
    // Direct counterpart to Java class
    constructor(public sortOrientationList: ColumnSortOrientation[]) {}
    public length(): number { return this.sortOrientationList.length; }
    public get(i: number): ColumnSortOrientation { return this.sortOrientationList[i]; }

    public getSchema(): Schema {
        return this.sortOrientationList.map((c) => c.columnDescription);
    }

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

    public toggle(col: string): RecordOrder {
        const result = this.clone();
        const index = result.find(col);
        if (index === -1)
            return this;
        result.sortOrientationList[index].isAscending =
            !result.sortOrientationList[index].isAscending;
        return result;
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

export interface StringColumnFilterDescription {
    colName: string;
    stringFilterDescription: StringFilterDescription;
}

export interface StringColumnsFilterDescription {
    colNames: string[];
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
    /**
     * If the column is an interval the interval is given by doubleValue - intervalEnd.
     */
    intervalEnd: number;
    comparison: Comparison;
}
