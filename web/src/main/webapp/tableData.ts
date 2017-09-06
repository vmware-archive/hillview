/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// I can't use an enum for ContentsKind because JSON deserialization does not
// return an enum from a string.
import {BasicColStats} from "./histogramBase";
export type ContentsKind = "Category" | "Json" | "String" | "Integer" | "Double" | "Date" | "Interval";

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
}

// Direct counterpart to Java class
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

// Direct counterpart to Java class
export interface Schema {
    [index: number] : IColumnDescription;
    length: number;
}

export interface RowView {
    count: number;
    values: any[];
}

// Direct counterpart to Java class
export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

export interface IDistinctStrings {
    uniqueStrings: string[];
    // This may be true if there are too many distinct strings in a column.
    truncated: boolean;
    // Number of values in the column containing the strings.
    columnSize: number;
}

/**
 * All strings that can appear in a categorical column.
 */
export class DistinctStrings implements IDistinctStrings {
    public uniqueStrings: string[];
    // This may be true if there are too many distinct strings in a column.
    public truncated: boolean;
    // Number of values in the column containing the strings.
    public columnSize: number;

    public constructor(ds: IDistinctStrings) {
        this.uniqueStrings = ds.uniqueStrings;
        this.truncated = ds.truncated;
        this.columnSize = ds.columnSize;
        this.uniqueStrings.sort();
    }

    public getStats(): BasicColStats {
        return {
            momentCount: 0,
            min: 0,
            max: this.uniqueStrings.length - 1,
            minObject: this.uniqueStrings[0],
            maxObject: this.uniqueStrings[this.uniqueStrings.length - 1],
            moments: [],
            presentCount: this.columnSize,
            missingCount: 0
        };
    }

    /**
     * Returns all strings numbered between min and max.
     * @param min    Minimum string number
     * @param max    Maximum string number
     * @param bucketCount
     * @returns {string[]}
     */
    public categoriesInRange(min: number, max: number, bucketCount: number): string[] {
        let boundaries: string[] = null;
        max = Math.floor(max);
        min = Math.ceil(min);
        let range = max - min;
        if (range <= 0)
            bucketCount = 1;

        if (this.uniqueStrings != null) {
            if (bucketCount >= range) {
                boundaries = this.uniqueStrings.slice(min, max + 1);  // slice end is exclusive
            } else {
                boundaries = [];
                for (let i = 0; i <= bucketCount; i++) {
                    let index = min + Math.round(i * range / bucketCount);
                    boundaries.push(this.uniqueStrings[index]);
                }
            }
        }
        return boundaries;
    }

    public get(index: number): string {
        index = Math.round(<number>index);
        if (index >= 0 && index < this.uniqueStrings.length)
            return this.uniqueStrings[index];
        return null;
    }
}

// Direct counterpart to Java class
export class RecordOrder {
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