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

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

/**
 * Direct counterpart of corresponding Java class
 */
export class Converters {
    public static dateFromDouble(value: number): Date {
        return new Date(value);
    }

    public static doubleFromDate(value: Date): number {
        return value.getTime();
    }
}

/**
 * Random seed management
  */
export class Seed {
    public static instance: Seed = new Seed();

    constructor() {}

    public get(): number {
        return Math.round(Math.random() * 1024 * 1024);
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
    let n = Math.abs(num);
    let zeros = Math.max(0, length - Math.floor(n).toString().length );
    let zeroString = Math.pow(10,zeros).toString().substr(1);
    if (num < 0) {
        zeroString = '-' + zeroString;
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
    let year = d.getFullYear();
    let month = d.getMonth() + 1;
    let day = d.getDate();
    let hour = d.getHours();
    let minutes = d.getMinutes();
    let seconds = d.getSeconds();
    let ms = d.getMilliseconds();
    let df = String(year) + "/" + zeroPad(month, 2) + "/" + zeroPad(day, 2);
    let suffix = "";
    if (ms != 0)
        suffix = "." + zeroPad(ms, 3);
    if (seconds != 0 || suffix != "")
        suffix = ":" + zeroPad(seconds, 2) + suffix;
    if (minutes != 0 || suffix != "")
        suffix = ":" + zeroPad(minutes, 2) + suffix;
    if (hour != 0 || suffix != "") {
        if (suffix == "")
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
    return text.replace(/[^a-zA-Z0-9]/g, '_');
}

/**
 * Convert a number to an html string by keeping only the most significant digits
 * and adding a suffix.
 */
export function significantDigits(n: number): string {
    let suffix = "";
    if (n == 0)
        return "0";
    let absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    } else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    } else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    } else if (absn > 5e3) {
        // This will prevent many year values from being converted
        suffix = "K";
        n = n / 1e3;
    } else if (absn < .001) {
        let expo = 0;
        while (n < .1) {
            n = n * 10;
            expo++;
        }
        suffix = "* 10<sup>-" + expo + "</sup>"
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

/**
 * This class builds some useful iterators over typescript enums.
 * In all these methods e is an enum *type*
 */
export class EnumIterators {
    static getNamesAndValues<T extends number>(e: any) {
        return EnumIterators.getNames(e).map(n => ({ name: n, value: e[n] as T }));
    }

    static getNames(e: any) {
        return EnumIterators.getObjValues(e).filter(v => typeof v == "string") as string[];
    }

    static getValues<T extends number>(e: any) {
        return EnumIterators.getObjValues(e).filter(v => typeof v == "number") as T[];
    }

    private static getObjValues(e: any): (number | string)[] {
        return Object.keys(e).map(k => e[k]);
    }
}

/**
 * Transpose a matrix.
 */
export function transpose<D>(m: D[][]): D[][] {
    let w = m.length;
    if (w == 0)
        return m;
    let h = m[0].length;

    let result: D[][] = [];
    for (let i=0; i < h; i++) {
        let v = [];
        for (let j=0; j < w; j++)
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
export function regression(data: number[][]) : number[] {
    let width = data.length;
    let height = data[0].length;
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
    let denom = ((size * sumt2) - (sumt * sumt));
    if (denom == 0)
    // TODO: should we use here some epsilon?
        return [];
    let a = 1 / denom;
    let  alpha = a * ((sumt2 * sumb) - (sumt * sumtb));
    let beta = a * ((size * sumtb) - (sumt * sumb));
    // estimation is alpha + beta * i
    return [alpha, beta];
}

/**
 * Truncate a string to the specified length, adding ellipses if it was too long.
  */
export function truncate(str: string, length: number): string {
    if (str.length > length) {
        return str.slice(0, length) + "..."
    } else {
        return str;
    }
}

export function clamp(x: number, xMin: number, xMax: number) {
    return Math.max(Math.min(x, xMax), xMin);
}

export function isInteger(n: number) {
    return Math.floor(n) == n;
}

/**
 * Convert a copy of an array.
 */
export function cloneArray<T>(arr: T[]): T[] {
    return arr.slice(0);
}

/**
 * Convert a set to an array
 */
export function cloneSet<T>(set: Set<T>): T[] {
    let ret: T[] = [];
    set.forEach((val) => ret.push(val));
    return ret;
}

export function exponentialDistribution(lambda: number) {
    return -Math.log(1 - Math.random()) / lambda;
}

export class PartialResult<T> {
    constructor(public done: number, public data: T) {}
}

export interface RpcReply {
    result: string;     // JSON or error message
    requestId: number;  // request that is being replied
    isError: boolean;
}

export interface ICancellable {
    /**
     * return 'true' if cancellation succeeds.
     * Cancellation may fail if the computation is terminated.
     */
    cancel(): boolean;
    /** time when operation was initiated */
    startTime(): Date;
}
