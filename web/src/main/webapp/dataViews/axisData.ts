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

import {
    axisBottom as d3axisBottom,
    axisLeft as d3axisLeft,
} from "d3-axis";
import {
    scaleLinear as d3scaleLinear,
    ScaleLinear as D3ScaleLinear,
    scaleTime as d3scaleTime,
    ScaleTime as D3ScaleTime,
} from "d3-scale";
import {DistinctStrings} from "../distinctStrings";
import {
    CategoricalValues,
    ColumnAndRange, DataRange,
    IColumnDescription,
    kindIsString
} from "../javaBridge";
import {Converters, significantDigits} from "../util";

export type AnyScale = D3ScaleLinear<number, number> | D3ScaleTime<number, number>;

/**
 * A D3 axis and an associated scale.
 */
export interface ScaleAndAxis {
    scale: AnyScale;
    axis: any;  // a d3 axis, but typing does not work well
}

/**
 * Contains all information required to build an axis and a d3 scale associated to it.
 */
export class AxisData {
    public constructor(public description: IColumnDescription,
                       public range: DataRange | null,
                       public distinctStrings: DistinctStrings,
                       public bucketCount: number) {}

    public scaleAndAxis(length: number, bottom: boolean, legend: boolean): ScaleAndAxis {
        const axisCreator = bottom ? d3axisBottom : d3axisLeft;

        let actualMin = this.range.min;
        let actualMax = this.range.max;
        let adjust = .5;
        if (legend && (this.description.kind === "Integer" ||
            kindIsString(this.description.kind))) {
            // These were adjusted, bring them back.
            actualMin += .5;
            actualMax -= .5;
            adjust = 0;
        }

        // on vertical axis the direction is swapped
        const domain = bottom ? [actualMin, actualMax] : [actualMax, actualMin];

        let axis: any;
        let scale: AnyScale;
        switch (this.description.kind) {
            case "Integer":
            case "Double": {
                scale = d3scaleLinear()
                    .domain(domain)
                    .range([0, length]);
                axis = axisCreator(scale);
                break;
            }
            case "Json":
            case "String":
            case "Category": {
                const ticks: number[] = [];
                const labels: string[] = [];
                // note: this is without adjustment.
                const tickCount = Math.ceil(this.range.max - this.range.min);
                // TODO: if the tick count is too large it must be reduced
                const minLabelWidth = 40;  // pixels
                const maxLabelCount = length / minLabelWidth;
                const labelPeriod = Math.ceil(tickCount / maxLabelCount);
                // On a legend the leftmost and rightmost ticks are at the ends
                // On a plot axis the ticks are offset .5 from the ends.
                const totalIntervals = legend ? (tickCount - 1) : tickCount;
                const tickWidth = length / totalIntervals;

                for (let i = 0; i < tickCount; i++) {
                    ticks.push((i + adjust) * tickWidth);
                    let label = "";
                    if (i % labelPeriod === 0) {
                        label = this.distinctStrings.get(this.range.min + .5 + i, false);
                        if (label === null)
                            label = "";
                    }
                    labels.push(label);
                }
                if (!bottom)
                    labels.reverse();

                // We manually control the ticks.
                const manual = d3scaleLinear()
                    .domain([0, length])
                    .range([0, length]);
                scale = d3scaleLinear()
                    .domain(domain)
                    .range([0, length]);
                axis = axisCreator(manual)
                    .tickValues(ticks)
                    .tickFormat((d, i) => labels[i]);
                break;
            }
            case "Date": {
                const minDate: Date = Converters.dateFromDouble(domain[0]);
                const maxDate: Date = Converters.dateFromDouble(domain[1]);
                scale = d3scaleTime()
                    .domain([minDate, maxDate])
                    .range([0, length]);
                axis = axisCreator(scale);
                break;
            }
            default: {
                console.log("Unexpected data kind for axis" + this.description.kind);
                axis = null;
                scale = null;
                break;
            }
        }

        return { scale: scale, axis: axis };
    }

    public getCategoricalValues(): CategoricalValues {
        return new CategoricalValues(this.description.name,
            this.distinctStrings != null ? this.distinctStrings.uniqueStrings : null);
    }

    /**
     * The categorical values in the min-max range.
     * @param {number} bucketCount  Number of categories to return.
     * @returns {string[]}  An array of categories, or null if this is not a
     * categorical column.
     */
    public getCategoriesInRange(bucketCount: number): string[] {
        if (this.distinctStrings == null)
            return null;
        return this.distinctStrings.categoriesInRange(
                this.range.min, this.range.max, bucketCount);
    }

    /**
     * Creates a ColumnAndRange data structure from the AxisData, which
     * may be used to initiate a histogram computation.
     * @param {number} bucketCount  Number of buckets expected in histogram.
     */
    public getColumnAndRange(bucketCount: number): ColumnAndRange {
        return {
            columnName: this.description.name,
            min: this.range.min,
            max: this.range.max,
            bucketBoundaries: this.getCategoriesInRange(bucketCount),
            onStrings: kindIsString(this.description.kind)
        };
    }

    /**
     * Get the boundaries of the specified bucket.
     * @param {number} bucket  Bucket number.
     * @returns {[number]}     The left and right margins of this bucket.
     */
    public boundaries(bucket: number): [number, number] {
        if (bucket < 0 || bucket >= this.bucketCount)
            return null;
        const interval = (this.range.max - this.range.min) / this.bucketCount;
        const start = this.range.min + interval * bucket;
        const end = start + interval;
        return [start, end];
    }

    /**
     * @param {number} bucket  Bucket number.
     * @returns {string}  A description of the boundaries of the specified bucket.
     */
    public bucketDescription(bucket: number): string {
        if (bucket < 0 || bucket >= this.bucketCount)
            return "empty";
        let [start, end] = this.boundaries(bucket);
        let closeBracket = ")";
        if (end >= this.range.max)
            closeBracket = "]";
        switch (this.description.kind) {
            case "Integer":
                start = Math.ceil(start);
                end = Math.floor(end);
                if (end < start)
                    return "empty";
                else if (end === start)
                    return significantDigits(start);
                else
                    return "[" + significantDigits(start) + ", " + significantDigits(end) + closeBracket;
            case "Double":
                 return "[" + significantDigits(start) + ", " + significantDigits(end) + closeBracket;
            case "Category":
            case "String":
            case "Json": {
                start = Math.ceil(start);
                end = Math.floor(end);
                if (end < start)
                    return "empty";
                else if (end === start)
                    return this.distinctStrings.get(start, true);
                else
                    return "[" + this.distinctStrings.get(start, true) + ", " +
                        this.distinctStrings.get(end, true) + closeBracket;
            }
            case "Date": {
                const minDate: Date = Converters.dateFromDouble(start);
                const maxDate: Date = Converters.dateFromDouble(end);
                return "[" + minDate + ", " + maxDate + closeBracket;
            }
            default: {
                return "unknown";
            }
        }
    }
}
