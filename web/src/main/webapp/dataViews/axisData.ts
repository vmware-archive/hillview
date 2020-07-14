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

import {axisBottom as d3axisBottom, axisLeft as d3axisLeft,} from "d3-axis";
import {scaleLinear as d3scaleLinear, scaleTime as d3scaleTime,} from "d3-scale";
import {BucketsInfo, ContentsKind, IColumnDescription, kindIsString, RangeFilterDescription} from "../javaBridge";
import {assert, Converters, formatDate, formatNumber, significantDigits, truncate,} from "../util";
import {AnyScale, D3Axis, D3SvgElement, SpecialChars} from "../ui/ui";
import {SchemaClass} from "../schemaClass";

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
            case "Interval":
                return significantDigits(this.value as number);
            case "Date":
                return Converters.dateFromDouble(this.value as number).toString();
            case "Duration":
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

    /**
     * Convert to a string, but don't use more than this many characters.
     * @param maxChars  If this is 0 there is no limit.
     */
    public toString(maxChars: number): string {
        let result: string;

        if (this.left == null) {
            return "";
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

        result += truncate(this.left.toString(), maxChars / 2) +
            "," + truncate(this.right.toString(), maxChars / 2);
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

// This value indicates that some data does not fall within a bucket.
export const NoBucketIndex = null;

/**
 * Contains all information required to build an axis and a d3 scale associated to it.
 */
export class AxisData {
    public scale: AnyScale;
    public axis: AxisDescription;
    public displayRange: BucketsInfo; // the range used to draw the data; may be adjusted from this.dataRange

    public constructor(public description: IColumnDescription | null, // may be null for e.g., the Y col in a histogram
                       // dataRange is the original range of the data
                       public dataRange: BucketsInfo | null,
                       public bucketCount: number) {
        this.displayRange = dataRange;
        const kind = description == null ? null : description.kind;
        if (dataRange != null) {
            if (kindIsString(kind)) {
                this.displayRange = {
                    min: -.5,
                    max: dataRange.stringQuantiles.length - .5,
                    presentCount: dataRange.presentCount,
                    missingCount: dataRange.missingCount,
                    allStringsKnown: dataRange.allStringsKnown,
                    stringQuantiles: dataRange.stringQuantiles,
                    maxBoundary: dataRange.maxBoundary
                };
            } else if (kind === "Integer") {
                this.displayRange = {
                    min: dataRange.min - .5,
                    max: dataRange.max + .5,
                    presentCount: dataRange.presentCount,
                    missingCount: dataRange.missingCount
                };
            }
        }
        // These are set when we know the screen size.
        this.scale = null;
        this.axis = null;
    }

    public getDisplayNameString(schema: SchemaClass): string {
        return schema.displayName(this.description.name).displayName;
    }

    private static needsAdjustment(kind: ContentsKind): boolean {
        return kindIsString(kind) || kind === "Integer";
    }

    public getString(index: number, clamp: boolean): string {
        index = Math.round(index);
        if (clamp) {
            if (index < 0)
                index = 0;
            if (index >= this.dataRange.stringQuantiles.length)
                index = this.dataRange.stringQuantiles.length - 1;
        }
        if (index >= 0 && index < this.dataRange.stringQuantiles.length)
            return this.dataRange.stringQuantiles[index];
        return "";
    }

    private bucketIndexToStringIndex(bucketNumber: number): number {
        return Math.floor(bucketNumber * this.dataRange.stringQuantiles.length / this.bucketCount);
    }

    private bucketLeftString(bucketNumber: number, clamp: boolean): string {
        bucketNumber = Math.floor(bucketNumber);
        return this.getString(this.bucketIndexToStringIndex(bucketNumber), clamp);
    }

    /**
     * Map the axis to the screen.
     * @param pixels       Number of pixels spanned by the axis.
     * @param axisKind     What kind of axis this is.
     * @param labelRoom   Number of pixels available for labels (most useful for vertical axes)
     */
    public setResolution(pixels: number, axisKind: AxisKind, labelRoom: number): void {
        const bottom = axisKind !== AxisKind.Left;
        const axisCreator = bottom ? d3axisBottom : d3axisLeft;
        let actualMin = this.displayRange.min;
        let actualMax = this.displayRange.max;
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
            case "Interval":  // interval ranges are just scalars
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
                const tickCount = Math.ceil(this.displayRange.max - this.displayRange.min);
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
                const maxLabelWidthInChars = Math.floor(
                    bottom ? maxLabelWidthInPixels / fontWidth : labelRoom / fontWidth);
                console.assert(maxLabelWidthInChars > 2);
                let rotate = false;

                for (let i = 0; i < tickCount; i++) {
                    ticks.push((i + adjust) * tickSpan);
                    let label = "";
                    if (i % labelPeriod === 0) {
                        label = this.getString(this.displayRange.min + .5 + i, false);
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

    public getFilter(min: number, max: number): RangeFilterDescription {
        let iMin = this.invertToNumber(min);
        let iMax = this.invertToNumber(max);
        let sMin = this.invert(min);
        let sMax = this.invert(max);
        if (iMin > iMax) {
            const tmp = iMin;
            iMin = iMax;
            iMax = tmp;

            const sTmp = sMin;
            sMin = sMax;
            sMax = sTmp;
        }
        return {
            min: iMin,
            max: iMax,
            minString: sMin,
            maxString: sMax,
            cd: this.description,
            includeMissing: false // TODO
        };
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
        else if (this.description.kind === "Double" || this.description.kind == "Interval")
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
        } else if (this.description.kind === "Double" || this.description.kind == "Interval") {
            result = inv as number;
        } else if (this.description.kind === "Date") {
            result = Converters.doubleFromDate(inv as Date);
        }
        return result;
    }

    public bucketBoundaries(bucket: number): BucketBoundaries {
        if (bucket === this.bucketCount)
            return new BucketBoundaries(new BucketBoundary("missing", "String", true), null);
        if (bucket === null || bucket < 0 || bucket > this.bucketCount)
            return new BucketBoundaries(null, null);

        const valueKind = this.description.kind;
        if (kindIsString(this.description.kind)) {
            const left = this.bucketLeftString(bucket, false);
            const leftBoundary = new BucketBoundary(left, valueKind, true);
            if (bucket === this.bucketCount - 1) {
                if (left == this.displayRange.maxBoundary)
                    return new BucketBoundaries(leftBoundary, null);
                return new BucketBoundaries(leftBoundary,
                    new BucketBoundary(this.displayRange.maxBoundary, valueKind, true));
            } else {
                let right = this.bucketLeftString(bucket + 1, false);
                if (left == right)
                    return new BucketBoundaries(leftBoundary, null);
                if (this.displayRange.allStringsKnown) {
                    const leftStringIndex = this.bucketIndexToStringIndex(bucket);
                    const nextStringIndex = this.bucketIndexToStringIndex(bucket + 1);
                    if (nextStringIndex === leftStringIndex + 1)
                        // The right-open interval contains in fact a single point
                        return new BucketBoundaries(leftBoundary, null);
                    if (nextStringIndex == leftStringIndex + 2) {
                        // An interval with just two points: show the right point inclusive
                        right = this.getString(leftStringIndex + 1, true);
                        return new BucketBoundaries(leftBoundary,
                            new BucketBoundary(right, valueKind, true));
                    }
                }
                return new BucketBoundaries(
                    leftBoundary,
                    new BucketBoundary(right, valueKind, false));
            }
        }

        const interval = (this.displayRange.max - this.displayRange.min) / this.bucketCount;
        let start = this.displayRange.min + interval * bucket;
        let end = start + interval;
        const inclusive = end >= this.displayRange.max;
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
            case "Interval":
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
     * @param bucket   Bucket number.  The same as the number of buckets for the missing data buckets.
     * @param maxChars Maximum number of characters to use for description; if 0 it is unlimited.
     * @returns  A description of the boundaries of the specified bucket.
     */
    public bucketDescription(bucket: number | null, maxChars: number): string {
        return this.bucketBoundaries(bucket).toString(maxChars);
    }
}
