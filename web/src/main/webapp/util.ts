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

import * as FileSaver from "file-saver";
import {ErrorReporter} from "./ui/errReporter";
import {NotifyDialog} from "./ui/dialog";
import {HtmlString, Size} from "./ui/ui";
import {ContentsKind, kindIsNumeric, kindIsString} from "./javaBridge";

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

export function assert(condition: boolean, message?: string): void {
    console.assert(condition, message);  // tslint:disable-line
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
}

/**
 * Retrieves a node from the DOM starting from a CSS selector specification.
 * @param cssselector  Node specification as a CSS selector.
 * @returns The unique selected node.
 */
export function findElement(cssselector: string): HTMLElement | null {
    const val = document.querySelector(cssselector);
    return val as HTMLElement;
}

/**
 * Returns a span element containing the specified text.
 * @param text       Text to insert in span.
 * @param highlight  If true the span has class highlight.
 */
export function makeSpan(text: string | null, highlight: boolean): HTMLElement {
    const span = document.createElement("span");
    if (text != null)
        span.textContent = text;
    if (highlight)
        span.className = "highlight";
    return span;
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
 * Save data in a file on the local filesystem.
 * @param {string} filename  File to save data to.
 * @param {string} contents  Contents to write in file.
 */
export function saveAs(filename: string, contents: string): void {
    const blob = new Blob([contents], {type: "text/plain;charset=utf-8"});
    FileSaver.saveAs(blob, filename);
    const notify = new NotifyDialog("File has been saved.",
        "Look for file " + filename + " in the browser Downloads folder",
        "File has been saved");
    notify.show();
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
 * The suffix may be omitted if it is zero
 */
export function formatDate(d?: Date): string {
    if (d == null)
        d = new Date();
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
            suffix = ":00:00";
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
    } else if (absn > 10e4) {
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
    return new HtmlString(String(n)).appendString(suffix);
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
    } else if (absn > 10e4) {
        // Using 10^4 will prevent many year values from being converted
        suffix = "K";
        n = n / 1e3;
    } else if (absn < .001) {
        let expo = 0;
        while (n < .1) {
            n = n * 10;
            expo++;
        }
        suffix = "* 10e" + expo;
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    return String(n) + suffix;
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
        const index = Math.round(i * (data.length - 1) / count);
        boundaries.push(data[index]);
    }
    return boundaries;
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
 */
export function truncate(str: string, length: number): string {
    if (str.length > length) {
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

export function uuidv4(): string {
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

/**
 * Convert a value in the table to a string representation.
 * @param val                  Value to convert.
 * @param {ContentsKind} kind  Type of value.
 */
export function convertToString(val: any, kind: ContentsKind): string {
    if (val == null)
        return "";
    if (kindIsNumeric(kind))
        return String(val);
    else if (kind === "Date")
        return formatDate(Converters.dateFromDouble(val as number));
    else if (kindIsString(kind))
        return val as string;
    else
        return val.toString();  // TODO
}

export function px(dim: number): string {
    if (dim === 0)
        return dim.toString();
    return dim.toString() + "px";
}
