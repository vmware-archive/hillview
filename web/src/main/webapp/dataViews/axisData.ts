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
import {
    ContentsKind,
    DataRange,
    IColumnDescription,
    kindIsString
} from "../javaBridge";
import {
    assert,
    Converters,
    formatDate,
    formatNumber,
    significantDigits,
} from "../util";
import {AnyScale, D3Axis, D3SvgElement, SpecialChars} from "../ui/ui";
import {PlottingSurface} from "../ui/plottingSurface";

export enum AxisKind {
    Bottom,
    Left,
    Legend
}

/**
 * Describes the boundary of a bucket from a histogram.
 */
class BucketBoundary {
    constructor(
        public readonly value: number | string,
        public readonly kind: ContentsKind,
        public readonly inclusive: boolean) {
    }

    public toString(): string {
        switch (this.kind) {
            case "Json":
            case "String":
                return this.value.toString();
            case "Integer":
            case "Double":
                return significantDigits(this.value as number);
            case "Date":
                return Converters.dateFromDouble(this.value as number).toString();
            case "Interval":
                return this.value.toString();
        }
    }

    public getNumber(): number | null {
        if (kindIsString(this.kind))
            return null;
        return this.value as number;
    }

    public getString(): string | null {
        if (!kindIsString(this.kind))
            return null;
        return this.value as string;
    }
}

/**
 * Represents the left and right boundaries of a bucket from a histogram.
 */
class BucketBoundaries {
    constructor(
        /*
         * If the left bucket is null the right one must be null too.
         * This indicates an emtpy bucket.
         */
        public readonly left: BucketBoundary | null,
        /*
         * If null the bucket contains a single value, the left one, which must be inclusive.
         */
        public readonly right: BucketBoundary | null) {
        if (left == null)
            assert(right == null);
    }

    public toString(): string {
        let result: string;

        if (this.left == null) {
            return "*empty*";
        }

        if (this.right == null) {
            assert(this.left.inclusive);
            return this.left.toString();
        }
        if (this.left.inclusive) {
            result = "[";
        } else {
            result = "(";
        }
        result += this.left.toString() + "," + this.right.toString();
        if (this.right.inclusive) {
            result += "]";
        } else {
            result += ")";
        }
        return result;
    }

    public getMin(): BucketBoundary {
        return this.left;
    }

    public getMax(): BucketBoundary {
        if (this.right != null)
            return this.right;
        return this.left;
    }
}

export class AxisDescription {
    constructor(
        public readonly axis: D3Axis,
        public readonly majorTickPeriod: number,
        public readonly rotate: boolean,
        // The labels are only available for some axes (strings)
        public readonly majorLabels: string[] | null) {}

    public draw(onElement: D3SvgElement): D3SvgElement {
        const result = onElement.call(this.axis);
        if (this.rotate)
            result.selectAll("text")
                .style("text-anchor", "start")
                .attr("transform", () => `rotate(20)`);
        if (this.majorLabels != null)
            result.selectAll("text")
                .append("title")
                .text((d, i) => this.majorLabels[i]);
        if (this.majorTickPeriod !== 1)
            result.selectAll("line")
                .filter((d, i) => i % this.majorTickPeriod === 0)
                .attr("y2", 12);
        return result;
    }
}

/**
 * Contains all information required to build an axis and a d3 scale associated to it.
 */
export class AxisData {
    public readonly distinctStrings: string[];
    public scale: AnyScale;
    public axis: AxisDescription;
    public bucketCount: number;

    public constructor(public description: IColumnDescription,
                       public range: DataRange | null) {
        this.bucketCount = 0;
        let useRange = range;
        if (useRange != null) {
            if (kindIsString(description.kind)) {
                useRange = {
                    min: -.5,
                    max: range.leftBoundaries.length - .5,
                    presentCount: range.presentCount,
                    missingCount: range.missingCount,
                    allStringsKnown: range.allStringsKnown,
                    leftBoundaries: range.leftBoundaries,
                    maxBoundary: range.maxBoundary
                };
            } else if (description.kind === "Integer") {
                useRange = {
                    min: range.min - .5,
                    max: range.max + .5,
                    presentCount: range.presentCount,
                    missingCount: range.missingCount
                };
            }
        }
        this.range = useRange;
        const strings = range !== null ? range.leftBoundaries : null;
        this.distinctStrings = strings;
        // These are set when we know the screen size.
        this.scale = null;
        this.axis = null;
    }

    private static needsAdjustment(kind: ContentsKind): boolean {
        return kindIsString(kind) || kind === "Integer";
    }

    public setBucketCount(bucketCount: number): void {
        this.bucketCount = bucketCount;
    }

    public getString(index: number, clamp: boolean): string {
        index = Math.round(index);
        if (clamp) {
            if (index < 0)
                index = 0;
            if (index >= this.distinctStrings.length)
                index = this.distinctStrings.length - 1;
        }
        if (index >= 0 && index < this.distinctStrings.length)
            return this.distinctStrings[index];
        return null;
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
                this.axis = new AxisDescription(axisCreator(this.scale), 1, false, null);
                break;
            }
            case "Json":
            case "String": {
                const ticks: number[] = [];
                const labels: string[] = [];
                const fullLabels: string[] = [];
                const tickCount = Math.ceil(this.range.max - this.range.min);
                const minLabelSpace = 20;  // We reserve at least this many pixels
                const maxLabelCount = pixels / minLabelSpace;
                const labelPeriod = Math.ceil(tickCount / maxLabelCount);
                // On a legend the leftmost and rightmost ticks are at the ends
                // On a plot axis the ticks are offset .5 from the ends.
                const totalIntervals = axisKind === AxisKind.Legend ? (tickCount - 1) : tickCount;
                const tickSpan = pixels / totalIntervals;
                const maxLabelWidthInPixels = 180;  // X labels are rotated, so they can be wider
                // This is actually just a guess for the width
                // of a letter used to draw axes.  We use d3 axes, which
                // have a default font size of 10.  Normally we should
                // measure the size of a string, but this is much simpler.
                const fontWidth = 8;
                const maxLabelWidthInChars = Math.floor(bottom ?
                    maxLabelWidthInPixels / fontWidth :
                    // TODO: get rid of the hardwired leftMargin
                    PlottingSurface.leftMargin / fontWidth);
                console.assert(maxLabelWidthInChars > 2);
                let rotate = false;

                for (let i = 0; i < tickCount; i++) {
                    ticks.push((i + adjust) * tickSpan);
                    let label = "";
                    if (i % labelPeriod === 0) {
                        label = this.getString(this.range.min + .5 + i, false);
                        if (label === null)
                            label = "";
                        if (label.length * fontWidth > tickSpan * labelPeriod)
                            rotate = true;
                        if (label.length > maxLabelWidthInChars) {
                            label = label.substr(0, maxLabelWidthInChars - 1) +
                                SpecialChars.ellipsis;
                        }
                    }
                    fullLabels.push(label);
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
                const axis = axisCreator(manual)
                    .tickValues(ticks)
                    .tickFormat((d, i) => labels[i]);
                this.axis = new AxisDescription(axis, labelPeriod, rotate, fullLabels);
                break;
            }
            case "Date": {
                const minDate: Date = Converters.dateFromDouble(domain[0]);
                const maxDate: Date = Converters.dateFromDouble(domain[1]);
                this.scale = d3scaleTime()
                    .domain([minDate, maxDate])
                    .range([0, pixels]);
                this.axis = new AxisDescription(axisCreator(this.scale), 1, false, null);
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
        assert(this.scale != null, "invert called before setting the resolution");
        const inv = this.scale.invert(v);
        let result: string;
        if (this.description.kind === "Integer")
            result = formatNumber(Math.round(inv as number));
        else if (kindIsString(this.description.kind))
            result = this.getString(inv as number, true);
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

    public bucketBoundaries(bucket: number): BucketBoundaries {
        if (bucket < 0 || bucket >= this.bucketCount)
            return new BucketBoundaries(null, null);

        const valueKind = this.description.kind;
        if (kindIsString(this.description.kind)) {
            const left = this.getString(bucket, false);
            if (this.range.allStringsKnown)
                return new BucketBoundaries(new BucketBoundary(left, valueKind, true), null);
            if (bucket === this.bucketCount - 1)
                return new BucketBoundaries(
                    new BucketBoundary(left, valueKind, true),
                    new BucketBoundary(this.range.maxBoundary, valueKind, true));
            else
                return new BucketBoundaries(
                    new BucketBoundary(left, valueKind, true),
                    new BucketBoundary(this.getString(bucket + 1, false), valueKind, false));
        }

        const interval = (this.range.max - this.range.min) / this.bucketCount;
        let start = this.range.min + interval * bucket;
        let end = start + interval;
        const inclusive = end >= this.range.max;
        switch (valueKind) {
            case "Integer":
                start = Math.ceil(start);
                end = Math.floor(end);
                if (end < start)
                    return new BucketBoundaries(null, null);
                else if (end === start)
                    return new BucketBoundaries(new BucketBoundary(start, valueKind, true), null);
                else
                    return new BucketBoundaries(
                        new BucketBoundary(start, valueKind, true),
                        new BucketBoundary(end, valueKind, inclusive));
            case "Double":
            case "Date":
                return new BucketBoundaries(
                     new BucketBoundary(start, valueKind, true),
                     new BucketBoundary(end, valueKind, inclusive)
                 );
            default:
                assert(false, "Unhandled data type " + valueKind);
        }
    }

    /**
     * @param {number} bucket  Bucket number.
     * @returns {string}  A description of the boundaries of the specified bucket.
     */
    public bucketDescription(bucket: number): string {
        return this.bucketBoundaries(bucket).toString();
    }
}
