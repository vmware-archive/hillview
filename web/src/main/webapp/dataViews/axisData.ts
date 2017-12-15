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

import {d3} from "../ui/d3-modules";
import {ColumnDescription, BasicColStats, RangeInfo, ColumnAndRange} from "../javaBridge";
import {ScaleLinear, ScaleTime} from "d3";
import {Converters, significantDigits} from "../util";
import {DistinctStrings} from "../distinctStrings";

export type AnyScale = ScaleLinear<number, number> | ScaleTime<number, number>;

/**
 * A D3 axis and an associated scale.
 */
export interface ScaleAndAxis {
    scale: AnyScale,
    axis: any,  // a d3 axis, but typing does not work well
}

/**
 * Contains all information required to build an axis and a d3 scale associated to it.
 */
export class AxisData {
    public constructor(public description: ColumnDescription,
                       public stats: BasicColStats,
                       public distinctStrings: DistinctStrings,    // used only for categorical histograms
                       public bucketCount: number)
    {}

    public scaleAndAxis(length: number, bottom: boolean, legend: boolean): ScaleAndAxis {
        let axisCreator = bottom ? d3.axisBottom : d3.axisLeft;

        let actualMin = this.stats.min;
        let actualMax = this.stats.max;
        let adjust = .5;
        if (legend && (this.description.kind == "Integer" || this.description.kind == "Category")) {
            // These were adjusted, bring them back.
            actualMin += .5;
            actualMax -= .5;
            adjust = 0;
        }

        // on vertical axis the direction is swapped
        let domain = bottom ? [actualMin, actualMax] : [actualMax, actualMin];

        let axis: any;
        let scale: AnyScale;
        switch (this.description.kind) {
            case "Integer":
            case "Double": {
                scale = d3.scaleLinear()
                    .domain(domain)
                    .range([0, length]);
                axis = axisCreator(scale);
                break;
            }
            case "Category": {
                let ticks: number[] = [];
                let labels: string[] = [];
                // note: this is without adjustment.
                let tickCount = Math.ceil(this.stats.max - this.stats.min);
                // TODO: if the tick count is too large it must be reduced
                let minLabelWidth = 40;  // pixels
                let maxLabelCount = length / minLabelWidth;
                let labelPeriod = Math.ceil(tickCount / maxLabelCount);
                // On a legend the leftmost and rightmost ticks are at the ends
                // On a plot axis the ticks are offset .5 from the ends.
                let totalIntervals = legend ? (tickCount - 1) : tickCount;
                let tickWidth = length / totalIntervals;

                for (let i = 0; i < tickCount; i++) {
                    ticks.push((i + adjust) * tickWidth);
                    let label = "";
                    if (i % labelPeriod == 0)
                        label = this.distinctStrings.get(this.stats.min + .5 + i);
                    labels.push(label);
                }
                if (!bottom)
                    labels.reverse();

                // We manually control the ticks.
                let manual = d3.scaleLinear()
                    .domain([0, length])
                    .range([0, length]);
                scale = d3.scaleLinear()
                    .domain(domain)
                    .range([0, length]);
                axis = axisCreator(manual)
                    .tickValues(ticks)
                    .tickFormat((d, i) => labels[i]);
                break;
            }
            case "Date": {
                let minDate: Date = Converters.dateFromDouble(domain[0]);
                let maxDate: Date = Converters.dateFromDouble(domain[1]);
                scale = d3
                    .scaleTime()
                    .domain([minDate, maxDate])
                    .range([0, length]);
                axis = axisCreator(scale);
                break;
            }
            default: {
                axis = null;
                scale = null;
                break;
            }
        }

        return { scale: scale, axis: axis };
    }

    /**
     * @returns {RangeInfo} structure summarizing this data.
     */
    getRangeInfo(): RangeInfo {
        return new RangeInfo(this.description.name,
            this.distinctStrings != null ? this.distinctStrings.uniqueStrings : null);
    }

    /**
     * The categorical values in the min-max range.
     * @param {number} bucketCount  Number of categories to return.
     * @returns {string[]}  An array of categories, or null if this is not a
     * categorical column.
     */
    getCategoriesInRange(bucketCount: number): string[] {
        if (this.distinctStrings == null)
            return null;
        return this.distinctStrings.categoriesInRange(
                this.stats.min, this.stats.max, bucketCount);
    }

    /**
     * Creates a ColumnAndRange data structure from the AxisData, which
     * may be used to initiate a histogram computation.
     * @param {number} bucketCount  Number of buckets expected in histogram.
     */
    getColumnAndRange(bucketCount: number): ColumnAndRange {
        return {
            columnName: this.description.name,
            min: this.stats.min,
            max: this.stats.max,
            bucketBoundaries: this.getCategoriesInRange(bucketCount)
        };
    }

    /**
     * @param {number} bucket  Bucket number.
     * @returns {string}  A description of the boundaries of the specified bucket.
     */
    bucketDescription(bucket: number): string {
        if (bucket < 0 || bucket >= this.bucketCount)
            return "empty";
        let interval = (this.stats.max - this.stats.min) / this.bucketCount;
        let start = this.stats.min + interval * bucket;
        let end = start + interval;
        let closeBracket = ")";
        if (end >= this.stats.max)
            closeBracket = "]";
        switch (this.description.kind) {
            case "Integer":
                start = Math.ceil(start);
                end = Math.floor(end);
                if (end < start)
                    return "empty";
                else if (end == start)
                    return significantDigits(start);
                else
                    return "[" + significantDigits(start) + ", " + significantDigits(end) + closeBracket;
            case "Double":
                 return "[" + significantDigits(start) + ", " + significantDigits(end) + closeBracket;
            case "Category": {
                start = Math.ceil(start);
                end = Math.floor(end);
                if (end < start)
                    return "empty";
                else if (end == start)
                    return this.distinctStrings.get(start);
                else
                    return "[" + this.distinctStrings.get(start) + ", " + this.distinctStrings.get(end) + closeBracket;
            }
            case "Date": {
                let minDate: Date = Converters.dateFromDouble(start);
                let maxDate: Date = Converters.dateFromDouble(end);
                return "[" + minDate + ", " + maxDate + closeBracket;
            }
            default: {
                return "unknown";
            }
        }
    }
}
