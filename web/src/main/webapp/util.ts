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

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

export interface Point2D {
    x: number;
    y: number;
}

// Direct counterpart of corresponding Java class
export class Converters {
    public static dateFromDouble(value: number): Date {
        return new Date(value);
    }

    public static doubleFromDate(value: Date): number {
        return value.getTime();
    }
}

// Random seed management
export class Seed {
    public static instance: Seed = new Seed();

    constructor() {}

    public get(): number {
        return Math.round(Math.random() * 1024 * 1024);
    }
}

export function formatNumber(n: number): string {
    return n.toLocaleString();
}

export function percent(n: number): string {
    n = Math.round(n * 1000) / 10;
    return significantDigits(n) + "%";
}

export function removeAllChildren(h: HTMLElement): void {
    while (h.hasChildNodes())
        h.removeChild(h.lastChild);
}

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
    } else if (absn < 1e-12) {
        n = n * 1e15;
        suffix = "* 10^{-15}"
    } else if (absn < 1e-9) {
        suffix = "* 10^{-12}";
        n = n * 1e12;
    } else if (absn < 1e-6) {
        suffix = "* 10^{-9}";
        n = n * 1e9;
    } else if (absn < 1e-3) {
        suffix = "* 10^{-6}";
        n = n * 1e6;
    }
    n = Math.round(n * 100) / 100;
    return String(n) + suffix;
}

export function reorder(m: number, n: number): [number, number] {
    if (m < n)
        return [m, n];
    else
        return [n, m];
}

// This class builds some useful iterators over typescript enums.
// In all these methods e is an enum *type*
export class EnumIterators {
    static getNamesAndValues<T extends number>(e: any) {
        return EnumIterators.getNames(e).map(n => ({ name: n, value: e[n] as T }));
    }

    static getNames(e: any) {
        return EnumIterators.getObjValues(e).filter(v => typeof v === "string") as string[];
    }

    static getValues<T extends number>(e: any) {
        return EnumIterators.getObjValues(e).filter(v => typeof v === "number") as T[];
    }

    private static getObjValues(e: any): (number | string)[] {
        return Object.keys(e).map(k => e[k]);
    }
}

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

// given a set of values in a heat map this computes two coefficients for a
// linear regression from X to Y.  The result is an array with two numbers, the
// two coefficients.  If the regression is undefined, the coefficients array is empty.
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
    return Math.floor(n) === n;
}

export function cloneArray<T>(arr: T[]): T[] {
    return arr.slice(0);
}

export function cloneSet<T>(set: Set<T>): T[] {
    let ret: T[] = [];
    set.forEach((val) => ret.push(val));
    return ret;
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
    // return 'true' if cancellation succeeds.
    // Cancellation may fail if the computation is terminated.
    cancel(): boolean;
    startTime(): Date;
}
