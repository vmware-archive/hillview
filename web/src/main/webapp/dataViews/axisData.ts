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
    scaleTime as d3scaleTime,
} from "d3-scale";
import {DistinctStrings} from "../distinctStrings";
import {
    ContentsKind,
    DataRange,
    IColumnDescription,
    kindIsString
} from "../javaBridge";
import {Converters, formatDate, formatNumber, significantDigits} from "../util";
import {AnyScale, D3Axis} from "../ui/ui";

export enum AxisKind {
    Bottom,
    Left,
    Legend
}

/**
 * Contains all information required to build an axis and a d3 scale associated to it.
 */
export class AxisData {
    public readonly distinctStrings: DistinctStrings;
    public scale: AnyScale;
    public axis: D3Axis;
    public bucketCount: number;

    public constructor(public description: IColumnDescription,
                       public range: DataRange | null) {
        this.bucketCount = 0;
        let useRange = range;
        if (kindIsString(description.kind)) {
            useRange = {
                min: -.5,
                max: range.boundaries.length - .5,
                presentCount: range.presentCount,
                missingCount: range.missingCount
            };
        } else if (description.kind === "Integer") {
            useRange = {
                min: range.min - .5,
                max: range.max + .5,
                presentCount: range.presentCount,
                missingCount: range.missingCount
            };
        }
        this.range = useRange;
        const strings = range !== null ? range.boundaries : null;
        this.distinctStrings = new DistinctStrings(strings, description.name);
        // These can be set when we know the screen size.
        this.scale = null;
        this.axis = null;
    }

    private static needsAdjustment(kind: ContentsKind): boolean {
        return kindIsString(kind) || kind === "Integer";
    }

    public setBucketCount(bucketCount: number): void {
        this.bucketCount = bucketCount;
    }

    /**
     * Map the axis to the screen.
     * @param pixels       Number of pixels spanned by the axis.
     * @param axisKind     What kind of axis this is.
     */
    public setResolution(pixels: number, axisKind: AxisKind): void {
        const bottom = axisKind !== AxisKind.Left;
        const axisCreator = bottom ? d3axisBottom : d3axisLeft;
        let actualMin = this.range.min;
        let actualMax = this.range.max;
        let adjust = .5;
        if (axisKind === AxisKind.Legend && AxisData.needsAdjustment(this.description.kind)) {
            // These were adjusted, bring them back.
            actualMin += .5;
            actualMax -= .5;
            adjust = 0;
        }

        // on vertical axis the direction is swapped
        const domain = bottom ? [actualMin, actualMax] : [actualMax, actualMin];

        switch (this.description.kind) {
            case "Integer":
            case "Double": {
                this.scale = d3scaleLinear()
                    .domain(domain)
                    .range([0, pixels]);
                this.axis = axisCreator(this.scale);
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
                const maxLabelCount = pixels / minLabelWidth;
                const labelPeriod = Math.ceil(tickCount / maxLabelCount);
                // On a legend the leftmost and rightmost ticks are at the ends
                // On a plot axis the ticks are offset .5 from the ends.
                const totalIntervals = axisKind === AxisKind.Legend ? (tickCount - 1) : tickCount;
                const tickWidth = pixels / totalIntervals;

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
                    .domain([0, pixels])
                    .range([0, pixels]);
                this.scale = d3scaleLinear()
                    .domain(domain)
                    .range([0, pixels]);
                this.axis = axisCreator(manual)
                    .tickValues(ticks)
                    .tickFormat((d, i) => labels[i]);
                break;
            }
            case "Date": {
                const minDate: Date = Converters.dateFromDouble(domain[0]);
                const maxDate: Date = Converters.dateFromDouble(domain[1]);
                this.scale = d3scaleTime()
                    .domain([minDate, maxDate])
                    .range([0, pixels]);
                this.axis = axisCreator(this.scale);
                break;
            }
            default: {
                console.log("Unexpected data kind for axis" + this.description.kind);
                break;
            }
        }
    }

    /**
     * Given a mouse coordinate on a specified d3 scale, returns the corresponding
     * Real-world value.  Returns the result as a string.
     */
    public invert(v: number): string {
        const inv = this.scale.invert(v);
        let result: string;
        if (this.description.kind === "Integer")
            result = formatNumber(Math.round(inv as number));
        else if (kindIsString(this.description.kind))
            result = this.distinctStrings.get(inv as number, true);
        else if (this.description.kind === "Double")
            result = formatNumber(inv as number);
        else if (this.description.kind === "Date")
            result = formatDate(inv as Date);
        else
            result = inv.toString();
        return result;
    }

    public invertToNumber(v: number): number {
        const inv = this.scale.invert(v);
        let result: number = 0;
        if (this.description.kind === "Integer" || kindIsString(this.description.kind)) {
            result = Math.round(inv as number);
        } else if (this.description.kind === "Double") {
            result = inv as number;
        } else if (this.description.kind === "Date") {
            result = Converters.doubleFromDate(inv as Date);
        }
        return result;
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
