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
import {DragEventKind, HtmlString, pageReferenceFormat, Resolution, Size} from "./ui/ui";
import {
    AggregateDescription, BucketsInfo,
    ComparisonFilterDescription,
    ContentsKind,
    Groups, IColumnDescription,
    kindIsNumeric,
    kindIsString, NextKList,
    RangeFilterArrayDescription,
    RangeFilterDescription, RecordOrder,
    RowFilterDescription, RowValue,
    SampleSet,
    StringFilterDescription
} from "./javaBridge";
import {AxisData} from "./dataViews/axisData";
import {SchemaClass} from "./schemaClass";

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

export interface Triple<T1, T2, T3> extends Pair<T1, T2> {
    third: T3;
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

/**
 * Find el in the array a.  If found returns the index.  If not found returns the negative of
 * the index where the element would be inserted.
 * @param a  Sorted array of values.
 * @param el Value to search.
 * @param comparator  Comparator function for two values of type T.
 */
export function binarySearch<T>(a: T[], el: T, comparator: (e1: T, e2: T) => number): number {
    let m = 0;
    let n = a.length - 1;
    while (m <= n) {
        const k = (n + m) >> 1;
        const cmp = comparator(el, a[k]);
        if (cmp > 0) {
            m = k + 1;
        } else if (cmp < 0) {
            n = k - 1;
        } else {
            return k;
        }
    }
    return -m - 1;
}

export function dataRange(data: RowValue[], cd: IColumnDescription): BucketsInfo {
    const present = data.filter((e) => e !== null);
    let b: BucketsInfo = {
        allStringsKnown: false,
        max: 0,
        maxBoundary: "",
        min: 0,
        missingCount: data.length - present.length,
        presentCount: present.length,
        stringQuantiles: []
    };
    if (present.length === 0)
        return b;
    if (kindIsNumeric(cd.kind)) {
        const numbers = present.map((v) => v as number);
        b.min = numbers.reduce((a, b) => Math.min(a, b));
        b.max = numbers.reduce((a, b) => Math.max(a, b));
    } else {
        const strings = present.map((v) => v as string);
        const sorted = strings.sort();
        const unique = [];
        let previous = null;
        for (const c of sorted) {
            if (c == previous)
                continue;
            previous = c;
            unique.push(c);
        }
        if (unique.length < Resolution.max2DBucketCount) {
            b.stringQuantiles = unique;
            b.allStringsKnown = true;
        } else {
            for (let i = 0; i < Resolution.max2DBucketCount; i++)
                b.stringQuantiles!.push(unique[Math.round(i * unique.length / Resolution.max2DBucketCount)]);
            b.allStringsKnown = false;
        }
    }
    return b;
}

/**
 * A few static methods to export data to CSV formats.
 */
export class Exporter {
    public static histogram2DAsCsv(
        data: Groups<Groups<number>>, schema: SchemaClass, axis: AxisData[]): string[] {
        const lines: string[] = [];

        const yAxis = axis[1].description!.name;
        const xAxis = axis[0].description!.name;
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

    public static tableAsCsv(order: RecordOrder, schema: SchemaClass,
                             aggregates: AggregateDescription[] | null, nextKList: NextKList): string[] {
        let lines = [];
        let line = "count";
        for (const o of order.sortOrientationList)
            line += "," + JSON.stringify(o.columnDescription.name);
        if (aggregates != null)
            for (const a of aggregates) {
                // noinspection UnnecessaryLocalVariableJS
                const dn = a.cd.name;
                line += "," + JSON.stringify(a.agkind + "(" + dn + "))");
            }
        lines.push(line);

        for (let i = 0; i < nextKList.rows.length; i++) {
            const row = nextKList.rows[i];
            line = row.count.toString();
            for (let j = 0; j < row.values.length; j++) {
                const kind = order.sortOrientationList[j].columnDescription.kind;
                let a = Converters.valueToString(row.values[j], kind, false);
                if (kindIsString(kind))
                    a = JSON.stringify(a);
                line += "," + a;
            }
            if (nextKList.aggregates != null) {
                const agg = nextKList.aggregates[i];
                for (const v of agg) {
                    line += "," + v;
                }
            }
            lines.push(line);
        }
        return lines;
    }

    public static histogramAsCsv(data: Groups<number>, schema: SchemaClass, axis: AxisData): string[] {
        const lines: string[] = [];
        const colName = axis.description.name;
        let line = JSON.stringify(colName) + ",count";
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

    public static histogram3DAsCsv(
        data: Groups<Groups<Groups<number>>>, schema: SchemaClass, axis: AxisData[]): string[] {
        let lines: string[] = [];
        const gAxis = axis[2].description!.name;
        for (let g = 0; g < axis[2].bucketCount; g++) {
            const gl = Exporter.histogram2DAsCsv(data.perBucket[g], schema, axis);
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
        const gl = Exporter.histogram2DAsCsv(data.perMissing, schema, axis);
        lines.concat(gl.map(l => JSON.stringify(gAxis + " " + by) + "," + l));
        return lines;
    }

    public static quartileAsCsv(g: Groups<SampleSet>, schema: SchemaClass, axis: AxisData): string[] {
        const lines: string[] = [];
        let line = "";
        const axisName = axis.description.name;
        for (let x = 0; x < axis.bucketCount; x++) {
            const bx = axis.bucketDescription(x, 0);
            const l = JSON.stringify(axisName + " " + bx);
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
    public static dateFromDouble(value: number | null): Date | null {
        if (value == null)
            return null;
        return new Date(value);
    }

    // Javascript dates are always UTC
    public static localDateFromDouble(value: number | null): Date | null {
        if (value == null)
            return null;
        const offset = new Date(value).getTimezoneOffset();
        return new Date(value + offset * 60 * 1000);
    }

    public static timeFromDouble(value: number | null): Date | null {
        return Converters.localDateFromDouble(value);
    }

    public static doubleFromDate(value: Date | null): number | null {
        if (value === null)
            return null;
        return value.getTime();
    }

    public static doubleFromLocalDate(value: Date | null): number | null {
        if (value === null)
            return null;
        const offset = value.getTimezoneOffset();
        return value.getTime() - offset * 60 * 1000;
    }

    /**
     * Convert a value expressed in milliseconds to a time duration.
     */
    public static durationFromDouble(val: number): string {
        if (val === 0)
            return "0";
        const ms = Math.round(val % 1000);
        val = Math.floor(val / 1000);
        const sec = val % 60;
        val = Math.floor(val / 60);
        const min = val % 60;
        val = Math.floor(val / 60);
        const hours = val % 24;
        const days = Math.floor(val / 24);
        let result: string = "";
        if (days > 0)
            result = significantDigits(days) + " days";
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
     * @param val   Value to convert.
     * @param kind  Type of value.
     * @param human If true this is for human consumption, else it is for machine consumption.
     */
    public static valueToString(val: RowValue | null, kind: ContentsKind, human: boolean): string {
        if (val == null)
            // This should probably not happen
            return "missing";
        if (kindIsNumeric(kind)) {
            if (human)
                return significantDigits(val as number);
            else
                return String(val);
        } else if (kind === "Date") {
            return formatDate(Converters.dateFromDouble(val as number));
        } else if (kind === "Time" || kind === "Duration") {
            const time = Converters.timeFromDouble(val as number);
            return formatTime(time, true);
        } else if (kindIsString(kind)) {
            return val as string;
        } else if (kind == "Interval") {
            const arr = val as number[];
            return "[" + this.valueToString(arr[0], "Double", human) + ":" +
                this.valueToString(arr[1], "Double", human) + "]";
        } else if (kind === "LocalDate") {
            const date = Converters.localDateFromDouble(val as number);
            return formatDate(date);
        } else {
            assert(false);
        }
    }

    /**
     * Human-readable description of a filter.
     * @param f: filter to describe
     */
    public static filterDescription(f: RangeFilterDescription): string {
        const min = kindIsNumeric(f.cd.kind) ? f.min : f.minString;
        const max = kindIsNumeric(f.cd.kind) ? f.max : f.maxString;
        return "Filtered on " + f.cd.name + " in range " +
            Converters.valueToString(min, f.cd.kind, true) + " - " + Converters.valueToString(max, f.cd.kind, true);
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
            default:
                assertNever(event);
        }
        return result + " from " + pageReferenceFormat(pageId);
    }

    static comparisonFilterDescription(filter: ComparisonFilterDescription): string {
        const kind = filter.column.kind;
        let str;
        switch (kind) {
            case "Json":
            case "String":
                str = this.valueToString(filter.stringValue, kind, true);
                break;
            case "Integer":
            case "Double":
            case "Date":
            case "Time":
            case "Duration":
            case "LocalDate":
                str = this.valueToString(filter.doubleValue, kind, true);
                break;
            case "Interval":
                str = this.valueToString([filter.doubleValue!, filter.intervalEnd!], kind, true);
                break;
            case "None":
                str = null;
                break;
            default:
                assertNever(kind);
        }
        return filter.column.name + " " + filter.comparison + " " + str;
    }
}

/**
 * Retrieves a node from the DOM starting from a CSS selector specification.
 * @param cssselector  Node specification as a CSS selector.
 * @returns The unique selected node.
 */
export function findElementAny(cssselector: string): HTMLElement | null {
    const val = document.querySelector(cssselector);
    return val as HTMLElement;
}

export function findElement(cssselector: string): HTMLElement {
    return findElementAny(cssselector)!;
}

/**
 * Returns an input box element containing the specified text.
 * @param text       Text to insert as the value.
 */
export function makeInputBox(text: string | null): HTMLElement {
    const inputBox = document.createElement("input");
    if (text != null)
        inputBox.value = text;
    return inputBox;
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
    deserialize(data: object): T | null;
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

export function percent(numerator: number, denominator: number): number {
    if (denominator <= 0)
        return 0;
    return (numerator / denominator) * 100;
}

/**
 * Convert n to a string representing a percent value
 * where we keep at most one digit after the decimal point
 */
export function percentString(n: number): string {
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
export function formatDate(d: Date | null): string {
    if (d == null)
        return "missing";
    const year = d.getFullYear();
    const month = d.getMonth() + 1;
    const day = d.getDate();
    const df = String(year) + "/" + zeroPad(month, 2) + "/" + zeroPad(day, 2);
    const time = formatTime(d, false);
    if (time != "")
        return df + " " + time;
    return df;
}

export function assertNever(x: never): never {
    throw new Error("Unexpected object: " + x);
}

export function disableSuggestions(visible: boolean): void {
    const vis = visible ? "block" : "none";
    const suggestions = document.getElementById("suggestions");
    if (suggestions !== null)
        suggestions.style.display = vis;
    const anchor = document.getElementById("suggestions-anchor");
    if (anchor !== null)
        anchor.style.display = vis;
}

/**
 * This is a time encoded as a date.  Ignore the date part and just
 * return the time.
 * @param d date that only has a time component.
 * @param nonEmpty if true return 00:00 when the result is empty.
 */
export function formatTime(d: Date | null, nonEmpty: boolean): string {
    if (d === null)
        return "missing";
    const hour = d.getHours();
    const minutes = d.getMinutes();
    const seconds = d.getSeconds();
    const ms = d.getMilliseconds();
    let time = "";
    if (ms !== 0)
        time = "." + zeroPad(ms, 3);
    if (seconds !== 0 || time !== "")
        time = ":" + zeroPad(seconds, 2) + time;
    if (minutes !== 0 || time !== "")
        time = ":" + zeroPad(minutes, 2) + time;
    if (hour !== 0 || time !== "") {
        if (time === "")
            time = ":00";
        time = zeroPad(hour, 2) + time;
    }
    if (nonEmpty) {
        if (time == "")
            return "00:00";
    }
    return time;
}

/**
 * Converts a string into another string which can be used as a legal ID
 * for an element.
 * @param {string} text   Text to convert.
 * @returns {string}      A similar text where each non-alphabetic or numeric character
 *                        is replaced with an underscore.
 */
export function makeId(text: string): string {
    text = text.replace(/[^a-zA-Z0-9]/g, "_");
    if (!(text[0].match(/[a-z_]/i)))
        text = "I" + text;
    return text;
}

/**
 * Convert a number to an html string by keeping only the most significant digits
 * and adding a suffix.
 */
export function significantDigitsHtml(n: number): HtmlString {
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
        suffix = "&times;10<sup>-" + expo + "</sup>";
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    const result = new HtmlString(String(n));
    result.appendSafeString(suffix);
    return result;
}

export function add(a: number, b: number): number {
    return a + b;
}

export function all<T>(a: T[], f: (x: T) => boolean): boolean {
    return a.map(e => f(e)).reduce((a, b) => a && b);
}

export class GroupsClass<T> {
    constructor(public groups: Groups<T>) {}

    public map<S>(func: (t: T, index: number) => S): GroupsClass<S> {
        const m = func(this.groups.perMissing, this.groups.perBucket.length);
        const buckets = this.groups.perBucket.map(func);
        return new GroupsClass<S>({ perBucket: buckets, perMissing: m });
    }

    public reduce<S>(func: (s: S, t: T) => S, start: S): S {
        return func(this.groups.perBucket.reduce(func, start), this.groups.perMissing);
    }
}

export class Heatmap {
    constructor(public data: GroupsClass<GroupsClass<number>>) {}

    static create(g: Groups<Groups<number>>): Heatmap {
        return new Heatmap(new GroupsClass(g).map(g1 => new GroupsClass(g1).map(c => c)));
    }

    public bitmap(f: (a: number, i: number) => boolean): Heatmap {
        return new Heatmap(this.data.map(g => g.map((e, i) => f(e, i) ? 1 : 0)));
    }

    public map(f: (a: number) => number): Heatmap {
        // noinspection JSUnusedLocalSymbols
        return new Heatmap(this.data.map(g => g.map((e, i) => f(e))));
    }

    public sum(): number {
        return this.data.reduce(
            (n1, g) => n1 + g.reduce(
                (n0: number, n1: number) => n0 + n1, 0), 0);
    }

    /**
     * Keep only the buckets whose indexes are in this range.
     * @param yMin  minimum index.
     * @param yMax  maximum index; EXCLUSIVE.
     */
    public bucketsInRange(yMin: number, yMax: number): Heatmap {
        return new Heatmap(this.data.map(
            (g, _) => g.map( (n, i1) =>
                ((yMin <= i1) && (i1 < yMax)) ? n : 0
            )));
    }
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
    if (win != null)
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

export function roughTimeSpan(min: number, max: number): [number, string] {
    const start = Converters.dateFromDouble(min);
    const end = Converters.dateFromDouble(max);
    if (start == null || end == null)
        return [0, "none"];
    // In milliseconds
    const ms_per_day = 86_400_000;
    const distance = end.getTime() - start.getTime();
    const years = distance / (365 * ms_per_day);
    if (years >= 5)
        return [Math.ceil(years), "years"];
    const months = distance / (30 * ms_per_day);
    if (months >= 5)
        return [Math.ceil(months), "months"];
    const days = distance / ms_per_day;
    if (days >= 5)
        return [Math.ceil(days), "days"];
    const hours = distance / 3_600_000;
    if (hours >= 5)
        return [Math.ceil(hours), "hours"];
    return [Math.ceil(distance / 60_000), "minutes"];
}

export function optionToBoolean(value: boolean | undefined): boolean {
    if (value === undefined)
        return false;
    return value;
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
interface ISimpleCancellable<T> extends IRawCancellable {
    unused: T; // this is here just to force the typescript typechecker to check T
    // I don't understand why otherwise it doesn't
}

export interface ICancellable<T> extends ISimpleCancellable<PartialResult<T>> {}

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

    private static colorReg = new RegExp(/rgb\(\s*(\d+),\s*(\d+),\s*(\d+)\s*\)/);

    public toString(): string {
        return "rgb(" + Math.round(this.r * 255) + "," + Math.round(this.g * 255) + "," + Math.round(this.b * 255) + ")";
    }

    /**
     * Parse a color in the format rgb(r, g, b).
     */
    public static parse(s: string): Color | null {
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
export function periodicSamples(data: string[], count: number): string[] | null {
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

export function desaturateOutsideRange(c: ColorMap, x0: number, x1: number): ColorMap {
    const [min, max] = reorder(x0, x1);
    return (value) => {
        const color = c(value);
        if (value < min || value > max) {
            const cValue = Color.parse(color);
            if (cValue != null) {
                const b = cValue.brighten(4);
                return b.toString();
            }
        }
        return color;
    }
}
