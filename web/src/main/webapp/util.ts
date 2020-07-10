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

/**
 * This file contains various useful functions and classes.
 */

import {ErrorReporter} from "./ui/errReporter";
import {DragEventKind, HtmlString, pageReferenceFormat, Size} from "./ui/ui";
import {
    AggregateDescription,
    ComparisonFilterDescription,
    ContentsKind,
    Groups,
    kindIsNumeric,
    kindIsString,
    RangeFilterArrayDescription,
    RangeFilterDescription,
    RowFilterDescription,
    SampleSet,
    StringFilterDescription
} from "./javaBridge";
import {AxisData} from "./dataViews/axisData";
import {SchemaClass} from "./schemaClass";

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

export interface Two<T> extends Pair<T, T> {}

export function assert(condition: boolean, message?: string): asserts condition {
    console.assert(condition, message);  // tslint:disable-line
}

export function allBuckets<R>(data: Groups<R>): R[] {
    const result = cloneArray(data.perBucket);
    result.push(data.perMissing);
    return result;
}

export function zip<T, S, R>(a: T[], b: S[], f: (ae: T, be: S) => R): R[] {
    return a.map((ae, i) => f(ae, b[i]));
}

export function histogramAsCsv(data: Groups<number>, schema: SchemaClass, axis: AxisData): string[] {
    const lines: string[] = [];
    let line = JSON.stringify(schema.displayName(axis.description.name).displayName) + ",count";
    lines.push(line);
    for (let x = 0; x < data.perBucket.length; x++) {
        const bx = axis.bucketDescription(x, 0);
        const l = "" + JSON.stringify(bx) + "," + data.perBucket[x];
        lines.push(l);
    }
    line = "missing," + data.perMissing;
    lines.push(line);
    return lines;
}

export function histogram2DAsCsv(
    data: Groups<Groups<number>>, schema: SchemaClass, axis: AxisData[]): string[] {
    const lines: string[] = [];

    const yAxis = schema.displayName(axis[1].description.name);
    const xAxis = schema.displayName(axis[0].description.name);
    let line = "";
    for (let y = 0; y < axis[1].bucketCount; y++) {
        const by = axis[1].bucketDescription(y, 0);
        line += "," + JSON.stringify(yAxis + " " + by);
    }
    line += ",missing";
    lines.push(line);
    for (let x = 0; x < axis[0].bucketCount; x++) {
        const d = data.perBucket[x];
        const bx = axis[0].bucketDescription(x, 0);
        let l = JSON.stringify(xAxis + " " + bx);
        for (const y of d.perBucket)
            l += "," + y;
        l += "," + d.perMissing;
        lines.push(l);
    }
    line = "mising";
    for (const y of data.perMissing.perBucket)
        line += "," + y;
    line += "," + data.perMissing.perMissing;
    lines.push(line);
    return lines;
}

export function histogram3DAsCsv(
    data: Groups<Groups<Groups<number>>>, schema: SchemaClass, axis: AxisData[]): string[] {
    let lines = [];
    const gAxis = schema.displayName(axis[2].description.name);
    for (let g = 0; g < axis[2].bucketCount; g++) {
        const gl = histogram2DAsCsv(data.perBucket[g], schema, axis);
        const first = gl[0];
        const tail = gl.slice(1);
        if (g == 0) {
            // This is the header line
            lines.push("," + first);
        }
        const by = axis[2].bucketDescription(g, 0);
        lines = lines.concat(tail.map(l => JSON.stringify(gAxis + " " + by) + "," + l));
    }
    const by = "missing";
    const gl = histogram2DAsCsv(data.perMissing, schema, axis);
    lines.concat(gl.map(l => JSON.stringify(gAxis + " " + by) + "," + l));
    return lines;
}

export function quartileAsCsv(g: Groups<SampleSet>, schema: SchemaClass, axis: AxisData): string[] {
    const lines: string[] = [];
    let line = "";
    const axisName = schema.displayName(axis.description.name);
    for (let x = 0; x < axis.bucketCount; x++) {
        const bx = axis.bucketDescription(x, 0);
        const l = JSON.stringify( axisName + " " + bx);
        line += "," + l;
    }
    line += ",missing"
    lines.push(line);

    const data = allBuckets(g);
    line = "min";
    for (const d of data) {
        line += "," + ((d.count === 0) ? "" : d.min);
    }
    lines.push(line);

    line = "q1";
    for (const d of data) {
        line += "," + ((d.count === 0) ? "" : (d.samples.length > 0 ? d.samples[0] : ""));
    }
    lines.push(line);

    line = "median";
    for (const d of data) {
        line += "," + ((d.count === 0) ? "" : (d.samples.length > 1 ? d.samples[1] : ""));
    }
    lines.push(line);

    line = "q3";
    for (const d of data) {
        line += "," + ((d.count === 0) ? "" : (d.samples.length > 2 ? d.samples[2] : ""));
    }
    lines.push(line);

    line = "max";
    for (const d of data) {
        line += "," + ((d.count === 0) ? "" : d.max);
    }
    lines.push(line);

    line = "missing";
    for (const d of data) {
        line += "," + d.missing;
    }
    lines.push(line);
    return lines;
}

export function describeQuartiles(data: SampleSet): string[] {
    if (data.count == 0) {
        return ["0", "", "", "", "", "", ""];
    }
    const count = significantDigits(data.count);
    const min = significantDigits(data.min);
    const max = significantDigits(data.max);
    const q1 = significantDigits(data.samples[0]);
    const q2 = data.samples.length > 1 ? significantDigits(data.samples[1]) : q1;
    const q3 = data.samples.length > 2 ? significantDigits(data.samples[2]) : q2;
    const missing = significantDigits(data.missing);
    return [count, missing, min, q1, q2, q3, max];
}

/**
 * Direct counterpart of corresponding Java class
 */
export class Converters {
    public static dateFromDouble(value: number): Date | null {
        if (value == null)
            return null;
        return new Date(value);
    }

    public static doubleFromDate(value: Date | null): number | null {
        if (value == null)
            return null;
        return value.getTime();
    }

    /**
     * Convert a value expressed in milliseconds to a time interval.
     */
    public static intervalFromDouble(val: number): string {
        if (val === 0)
            return "0";
        const ms = val % 1000;
        val = Math.floor(val / 1000);
        const sec = val % 60;
        val = Math.floor(val / 60);
        const min = val % 60;
        val = Math.floor(val / 60);
        const hours = val % 24;
        const days = Math.floor(val / 24);
        let result: string = "";
        if (days > 0)
            result = significantDigits(days) + "days";
        if (days > 1000)
            return result;
        if (hours > 0) {
            if (result.length !== 0)
                result += " ";
            result += hours.toString() + "H";
        }
        if (min > 0) {
            if (result.length !== 0)
                result += " ";
            result += " " + min.toString() + "M";
        }
        let addedSec = false;
        if (sec > 0) {
            if (result.length !== 0)
                result += " ";
            addedSec = true;
            result += sec.toString();
        }
        if (ms > 0 && days === 0) {
            if (!addedSec)
                result += " 0";
            addedSec = true;
            result += "." + ms.toString();
        }
        if (addedSec)
            result += "s";
        return result;
    }

    /**
     * Convert a value in a table cell to a string representation.
     * @param val                  Value to convert.
     * @param {ContentsKind} kind  Type of value.
     */
    public static valueToString(val: any, kind: ContentsKind): string {
        if (val == null)
            // This should probably not happen
            return "missing";
        if (kindIsNumeric(kind))
            return String(val);
        else if (kind === "Date")
            return formatDate(Converters.dateFromDouble(val as number));
        else if (kindIsString(kind))
            return val as string;
        else
            return val.toString();  // TODO
    }

    /**
     * Human-readable description of a filter.
     * @param f: filter to describe
     */
    public static filterDescription(f: RangeFilterDescription): string {
        const min = kindIsNumeric(f.cd.kind) ? f.min : f.minString;
        const max = kindIsNumeric(f.cd.kind) ? f.max : f.maxString;
        return "Filtered on " + f.cd.name + " in range " +
            Converters.valueToString(min, f.cd.kind) + " - " + Converters.valueToString(max, f.cd.kind);
    }

    public static filterArrayDescription(f: RangeFilterArrayDescription): string {
        let result = "";
        if (f.complement)
            result = "not ";
        for (const filter of f.filters)
            result += Converters.filterDescription(filter);
        return result;
    }

    public static stringFilterDescription(f: StringFilterDescription): string {
        let result = "";
        if ((f.asSubString || f.asRegEx) && !f.complement)
            result += " contains ";
        else if ((f.asSubString || f.asRegEx) && f.complement)
            result += " does not contain ";
        else if (!(f.asSubString || f.asRegEx) && !f.complement)
            result += " equals ";
        else if (!(f.asSubString || f.asRegEx) && f.complement)
            result += " does not equal ";
        result += f.compareValue;
        return result;
    }

    public static rowFilterDescription(f: RowFilterDescription): string {
        let result = "Filter rows that are " + f.comparison + " than [";
        result += f.data.join(",");
        result += "]";
        result += " compared " +
            f.order.sortOrientationList.map(c => c.isAscending ? "asc" : "desc").join(",");
        return result;
    }

    /**
     * Human-readable description of a drag event.
     * @param pageId  Page where drag event originated.
     * @param event   Event kind.
     */
    public static eventToString(pageId: string, event: DragEventKind): string {
        let result = "Drag-and-drop ";
        switch (event) {
            case "Title":
                result += " contents ";
                break;
            case "XAxis":
                result += " X axis ";
                break;
            case "YAxis":
                result += " Y axis ";
                break;
            case "GAxis":
                result += " Grouping "
                break;
        }
        return result + " from " + pageReferenceFormat(pageId);
    }

    static comparisonFilterDescription(filter: ComparisonFilterDescription): string {
        const kind = filter.column.kind;
        return filter.column.name + " " + filter.comparison +
            (kindIsNumeric(kind) ? this.valueToString(filter.doubleValue, kind) :
                this.valueToString(filter.stringValue, kind));
    }
}

/**
 * Retrieves a node from the DOM starting from a CSS selector specification.
 * @param cssselector  Node specification as a CSS selector.
 * @param allowNull    If true allow null values to be found.  Default is false.
 * @returns The unique selected node.
 */
export function findElement(cssselector: string, allowNull?: boolean): HTMLElement | null {
    const val = document.querySelector(cssselector);
    if (allowNull == null || !allowNull)
        assert(val != null);
    return val as HTMLElement;
}

/**
 * Returns a span element containing the specified text.
 * @param text       Text to insert in span.
 * @param highlight  If true the span has class highlight.
 */
export function makeSpan(text: string | null, highlight: boolean = false): HTMLElement {
    const span = document.createElement("span");
    if (text != null)
        span.textContent = text;
    if (highlight)
        span.className = "highlight";
    return span;
}

export function valueWithConfidence(value: number, confidence: number | null): [number, number] {
    if (confidence != null) {
        return [Math.round(value - confidence), Math.round(value + confidence)];
    } else {
        return [value, value];
    }
}

export function makeInterval(value: [number, number]): string {
    if (value[0] >= value[1])
        return significantDigits(value[0]);
    else
        return significantDigits(value[0]) + ":" + significantDigits(value[1]);
}

/**
 * Creates an HTML element that displays as missing data.
 */
export function makeMissing(): HTMLElement {
    const span = document.createElement("span");
    span.textContent = "missing";
    span.className = "missingData";
    return span;
}

/**
 * Interface which is implemented by classes that know
 * how to serialize and deserialize themselves from objects.
 * T in general will be the class itself.
 */
export interface Serializable<T> {
    /**
     * Save the data into a javascript object.
     */
    serialize(): object;
    /**
     * Initialize the current object from the specified object.
     * @returns The same object if deserialization is successful, null otherwise.
     */
    deserialize(data: object): T;
}

/**
 * Load the contents of a text file.
 * @param file      File to load.
 * @param onsuccess method called with the file contents when successful.
 * @param reporter  Used to report errors.
 */
export function loadFile(file: File,
                         onsuccess: (s: string) => void,
                         reporter: ErrorReporter): void {
    const reader = new FileReader();
    reader.onloadend = () => onsuccess(reader.result as string);
    reader.onabort = () => reporter.reportError("Read of file " + file.name + " aborted");
    reader.onerror = (e) => reporter.reportError(e.toString());
    if (file)
        reader.readAsText(file);
    else
        reporter.reportError("Invalid file");
}

/**
 * Random seed management
 */
export class Seed {
    public static instance: Seed = new Seed();

    constructor() {}

    // noinspection JSMethodCanBeStatic
    public get(): number {
        return Math.round(Math.random() * 1024 * 1024);
    }

    public getSampled(samplingRate: number): number {
        if (samplingRate >= 1.0)
            return 0;
        return this.get();
    }
}

/**
 * Converts a number to a more readable representation.
 */
export function formatNumber(n: number): string {
    return n.toLocaleString();
}

export function prefixSum(n: number[]): number[] {
    let s = 0;
    const result = [];
    for (const c of n) {
        s += c;
        result.push(s);
    }
    return result;
}

/**
 * Convert n to a string representing a percent value
 * where we keep at most one digit after the decimal point
 */
export function percent(n: number): string {
    n = Math.round(n * 1000) / 10;
    return significantDigits(n) + "%";
}

/**
 * convert a number to a string and prepend zeros if necessary to
 * bring the integer part to the specified number of digits
 */
function zeroPad(num: number, length: number): string {
    const n = Math.abs(num);
    const zeros = Math.max(0, length - Math.floor(n).toString().length );
    let zeroString = Math.pow(10, zeros).toString().substr(1);
    if (num < 0) {
        zeroString = "-" + zeroString;
    }

    return zeroString + n;
}

/**
 * Write a date into a format like
 * 2017/03/05 10:05:30.243
 * The suffix may be omitted if it is zero.
 * This should match the algorithm in the Java Converters.toString(Instant) method.
 */
export function formatDate(d: Date): string {
    if (d == null)
        return "missing";
    const year = d.getFullYear();
    const month = d.getMonth() + 1;
    const day = d.getDate();
    const hour = d.getHours();
    const minutes = d.getMinutes();
    const seconds = d.getSeconds();
    const ms = d.getMilliseconds();
    const df = String(year) + "/" + zeroPad(month, 2) + "/" + zeroPad(day, 2);
    let suffix = "";
    if (ms !== 0)
        suffix = "." + zeroPad(ms, 3);
    if (seconds !== 0 || suffix !== "")
        suffix = ":" + zeroPad(seconds, 2) + suffix;
    if (minutes !== 0 || suffix !== "")
        suffix = ":" + zeroPad(minutes, 2) + suffix;
    if (hour !== 0 || suffix !== "") {
        if (suffix === "")
            suffix = ":00";
        suffix = " " + zeroPad(hour, 2) + suffix;
    }
    return df + suffix;
}

/**
 * Converts a string into another string which can be used as a legal ID
 * for an element.
 * @param {string} text   Text to convert.
 * @returns {string}      A similar text where each non-alphabetic or numeric character
 *                        is replaced with an underscore.
 */
export function makeId(text: string): string {
    return text.replace(/[^a-zA-Z0-9]/g, "_");
}

/**
 * Convert a number to an html string by keeping only the most significant digits
 * and adding a suffix.
 */
export function significantDigitsHtml(n: number): HtmlString {
    if (n === null)
        return null;
    let suffix = "";
    if (n === 0)
        return new HtmlString("0");
    const absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    } else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    } else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    } else if (absn > 1e4) {
        // Using 10^4 will prevent many year values from being converted
        suffix = "K";
        n = n / 1e3;
    } else if (absn < .001) {
        let expo = 0;
        while (n < .1) {
            n = n * 10;
            expo++;
        }
        suffix = "&times; 10<sup>-" + expo + "</sup>";
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    return new HtmlString(String(n)).appendSafeString(suffix);
}

export function add(a: number, b: number): number {
    return a + b;
}

/**
 * Convert a number to an string by keeping only the most significant digits
 * and adding a suffix.  Similar to the previous function, but returns a pure string.
 */
export function significantDigits(n: number): string {
    let suffix = "";
    if (n === 0)
        return "0";
    const absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    } else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    } else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    } else if (absn > 1e4) {
        // Using 10^4 will prevent many year values from being converted
        suffix = "K";
        n = n / 1e3;
    } else if (absn < .001) {
        let expo = 0;
        while (n < 1) {
            n = n * 10;
            expo++;
        }
        suffix = " * 10e-" + expo;
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    return String(n) + suffix;
}

/**
 * Order two numbers such that the smaller one comes first
 */
export function reorder(m: number, n: number): [number, number] {
    if (m < n)
        return [m, n];
    else
        return [n, m];
}

interface NameValue<T> {
    name: string;
    value: T;
}

export function sameAggregate(a: AggregateDescription, b: AggregateDescription): boolean {
    return a.agkind === b.agkind && a.cd.name === b.cd.name && a.cd.kind === b.cd.kind;
}

/**
 * Find a value in an array using a specified equality function.  Return index in array.
 * @param value         Value to search.
 * @param array         Array to search in.
 * @param comparison    Comparison function, returns true if two values are equal.
 */
export function find<T>(value: T, array: T[], comparison: (l: T, r: T) => boolean): number {
    for (let i = 0; i < array.length; i++)
        if (comparison(value, array[i]))
            return i;
    return -1;
}

/**
 * This class builds some useful iterators over typescript enums.
 * In all these methods enumType is an enum *type*
 */
export class EnumIterators {
    public static getNamesAndValues<T extends number>(enumType: any): Array<NameValue<T>> {
        return EnumIterators.getNames(enumType).map((n) => ({ name: n, value: enumType[n] as T }));
    }

    public static getNames(enumType: any): string[] {
        return EnumIterators.getObjValues(enumType).filter((v) => typeof v === "string") as string[];
    }

    public static getValues<T extends number>(enumType: any): T[] {
        return EnumIterators.getObjValues(enumType).filter((v) => typeof v === "number") as T[];
    }

    private static getObjValues(enumType: any): Array<number | string> {
        return Object.keys(enumType).map((k) => enumType[k]);
    }
}

/**
 * Transpose a matrix.
 */
export function transpose<D>(m: D[][]): D[][] {
    const w = m.length;
    if (w === 0)
        return m;
    const h = m[0].length;

    const result: D[][] = [];
    for (let i = 0; i < h; i++) {
        const v = [];
        for (let j = 0; j < w; j++)
            v.push(m[j][i]);
        result.push(v);
    }
    return result;
}

/**
 * Converts a map to an array by creating an array with an even number of elements
 * where the elements alternate keys and values [k0, v0, k1, v1, ...]
 */
export function mapToArray<K, V>(map: Map<K, V>): any[] {
    const res: any[] = [];
    map.forEach((v, k) => { res.push(k); res.push(v); });
    return res;
}

/**
 * Given a set of values in a heatmap this computes two coefficients for a
 * linear regression from X to Y.  The result is an array with two numbers, the
 * two coefficients.  If the regression is undefined, the coefficients array is empty.
 */
export function regression(data: number[][]): number[] {
    const width = data.length;
    const height = data[0].length;
    let sumt = 0;
    let sumt2 = 0;
    let sumb = 0;
    let sumtb = 0;
    let size = 0;
    for (let i = 0; i < width; i++) {
        for (let j = 0; j < height; j++) {
            sumt += i * data[i][j];
            sumt2 += i * i * data[i][j];
            sumb += j * data[i][j];
            sumtb += i * j * data [i][j];
            size += data[i][j];
        }
    }
    const denom = ((size * sumt2) - (sumt * sumt));
    if (denom === 0)
    // TODO: should we use here some epsilon?
        return [];
    const a = 1 / denom;
    const  alpha = a * ((sumt2 * sumb) - (sumt * sumtb));
    const beta = a * ((size * sumtb) - (sumt * sumb));
    // estimation is alpha + beta * i
    return [alpha, beta];
}

/**
 * This is actually just a guess on the width of the vertical scroll-bar,
 * which we would like to always be visible.
 */
export const scrollBarWidth = 15;

/**
 * Browser window size excluding scrollbars.
 */
export function browserWindowSize(): Size {
    return {
        width: window.innerWidth - scrollBarWidth,
        height: window.innerHeight,
    };
}

export function openInNewTab(url: string): void {
    const win = window.open(url, "_blank");
    win.focus();
}

/**
 * Truncate a string to the specified length, adding ellipses if it was too long.
 * @param str     String to truncate.
 * @param length  Maximum length; if zero there is no truncation.
 */
export function truncate(str: string, length: number): string {
    if (length > 0 && str.length > length) {
        return str.slice(0, length) + "...";
    } else {
        return str;
    }
}

export function clamp(value: number, min: number, max: number): number {
    return Math.max(Math.min(value, max), min);
}

export function isInteger(n: number): boolean {
    return Math.floor(n) === n;
}

/**
 * Copy an array.
 */
export function cloneArray<T>(arr: T[]): T[] {
    return arr.slice(0);
}

export function cloneToSet<T>(arr: T[]): Set<T> {
    const result = new Set<T>();
    arr.forEach((e) => result.add(e));
    return result;
}

export function getUUID(): string {
    // From https://stackoverflow.com/questions/105034/create-guid-uuid-in-javascript
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
        const r = Math.random() * 16 | 0;
        const v = c === "x" ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

export function readableTime(millisec: number): string {
    let seconds = Math.floor(millisec / 1000);
    let minutes = Math.floor(seconds / 60);
    const hours = Math.floor(seconds / 3600);
    seconds = seconds % 60;
    minutes = minutes % 60;

    function pad(n: number): string {
        if (n < 10)
            return "0" + n;
        else
            return n.toString();
    }

    const min = pad(minutes);
    const sec = pad(seconds);

    if (hours > 0) {
        return `${hours}:${min}:${sec}`;
    }
    return `${min}:${sec}`;
}

/**
 * Convert a set to an array
 */
export function cloneSet<T>(set: Set<T>): T[] {
    const ret: T[] = [];
    set.forEach((val) => ret.push(val));
    return ret;
}

export function exponentialDistribution(lambda: number): number {
    return -Math.log(1 - Math.random()) / lambda;
}

export class PartialResult<T> {
    constructor(public done: number, public data: T) {}
}

export interface RpcReply {
    result: string;     // JSON or error message.
    requestId: number;  // Request that is being replied.
    isError: boolean;   // Indicates that the message contains an error.
    isCompleted: boolean;  // If true this message is the last one.
}

// untyped cancellable
export interface IRawCancellable {
    /**
     * return 'true' if cancellation succeeds.
     * Cancellation may fail if the computation is terminated.
     */
    cancel(): boolean;
    /** time when operation was initiated */
    startTime(): Date;
}

// Typed version of the cancellable API, makes it easy to do
// strong typing.
export interface ICancellable<T> extends IRawCancellable {}

export function px(dim: number): string {
    if (dim === 0)
        return dim.toString();
    return dim.toString() + "px";
}

/**
 * A color triple normalized in the range 0-1 for each component.
 */
export class Color {
    public constructor(public readonly r: number,
                       public readonly g: number,
                       public readonly b: number) {
        if (r < 0 || r > 1 || g < 0 || g > 1 || b < 0 || b > 1)
            throw new Error("Color out of range: " + r +"," + g + "," + b)
    }

    private static colorReg = new RegExp(/rgb\((\d+), (\d+), (\d+)\)/);

    public toString(): string {
        return "rgb(" + Math.round(this.r * 255) + "," + Math.round(this.g * 255) + "," + Math.round(this.b * 255) + ")";
    }

    /**
     * Parse a color in the format rgb(r, g, b).
     */
    public static parse(s: string): Color {
        const m = Color.colorReg.exec(s);
        if (m == null)
            return null;
        return new Color(+m[1]/255, +m[2]/255, +m[3]/255);
    }

    public brighten(amount: number): Color {
        return new Color(
            (this.r + (amount - 1)) / amount,
            (this.g + (amount - 1)) / amount,
            (this.b + (amount - 1)) / amount);
    }
}

/**
 * Given some strings returns a subset of them.
 * @param data   A set of strings.
 * @param count  Number of strings to return.
 * @returns      At most count strings equi-spaced.
 */
export function periodicSamples(data: string[], count: number): string[] {
    if (data == null)
        return null;

    if (count >= data.length)
        return data;
    const boundaries: string[] = [];
    for (let i = 0; i < count; i++) {
        // This formula sets the first bucket left boundary at .5 and the last at (data.length - 1)+ .5
        const index = Math.ceil(i * data.length / count - .5);
        console.assert(index >= 0 && index < data.length);
        boundaries.push(data[index]);
    }
    return boundaries;
}

export type ColorMap = (d: number) => string;

export function desaturateOutsideRange(c: ColorMap, min: number, max: number): ColorMap {
    return (value) => {
        const color = c(value);
        if (value < min || value > max) {
            const cValue = Color.parse(color);
            const b = cValue.brighten(4);
            return b.toString();
        }
        return color;
    }
}