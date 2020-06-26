/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {pie as d3pie, arc as d3arc} from "d3-shape";
import {AxisData} from "../dataViews/axisData";
import {Groups, kindIsString} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {SpecialChars} from "./ui";
import {
    add, allBuckets,
    formatNumber,
    makeInterval,
    percent,
    significantDigits,
    Two,
    valueWithConfidence
} from "../util";

interface ValueAndIndex {
    value: number;
    index: number;
}

/**
 * A PiePlot draws a histogram as a pie chart on a PlottingSurface.
 */
export class PiePlot extends Plot<Two<Groups<number>>> {
    /**
     * Sampling rate that was used to compute the histogram.
     */
    public samplingRate: number;
    public isPrivate: boolean;
    public maxYAxis: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    /**
     * Set the histogram that we want to draw.
     * @param bars          Description of the histogram bars.
     * @param samplingRate  Sampling rate used to compute this histogram.
     * @param axisData      Description of the X axis.
     * @param maxYAxis      Not used for pie chart.
     * @param isPrivate     True if we are plotting private data.
     */
    public setHistogram(bars: Two<Groups<number>>, samplingRate: number,
                        axisData: AxisData, maxYAxis: number | null, isPrivate: boolean): void {
        this.data = bars;
        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        this.isPrivate = isPrivate;
        this.maxYAxis = maxYAxis;
    }

    private color(index: number, count: number): string {
        if (index == count - 1)
            return "white";  // missing
        if (kindIsString(this.xAxisData.description.kind))
            return Plot.categoricalMap(index);
        return Plot.colorMap(index / count);
    }

    private label(bucketIndex: number): string {
        if (bucketIndex == this.xAxisData.bucketCount)
            return "missing";
        return this.xAxisData.bucketDescription(bucketIndex, 40);
    }

    private countAsString(count: number, confidence: number, sum: number): string {
        let result;
        if (this.isPrivate) {
            result = SpecialChars.approx + makeInterval(valueWithConfidence(count, confidence));
        } else if (this.samplingRate < 1) {
            result = SpecialChars.approx + significantDigits(count);
        } else {
            result = formatNumber(count);
        }
        result += ", ";
        if (this.isPrivate) {
            const min = Math.max(0, count - confidence);
            const percLow = percent(min/sum);
            const percHigh = percent((count + confidence) / sum);
            if (percLow === percHigh)
                result += percLow;
            else
                result += percent(min / sum) + ":" + percent((count + confidence) / sum);
        } else {
            result += percent(count / sum);
        }
        return result;
    }

    private static clamp(value: number, min: number, max: number): number {
        if (min > max) {
            // This should not happen.
            return min;
        } else {
            if (value < min)
                return min;
            else if (value > max)
                return max;
        }
        return value;
    }

    private drawPie(): void {
        const counts = this.data.first.perBucket.map((x) => Math.max(x, 0));
        counts.push(this.data.first.perMissing);
        const pie = d3pie().sort(null);
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        const radius = Math.min(chartWidth, chartHeight) / 2.2;
        const arc = d3arc()
            .innerRadius(0)
            .outerRadius(radius);
        let confidence;
        if (this.isPrivate) {
            confidence = allBuckets(this.data.second);
        } else {
            confidence = new Array(this.data.first.perBucket.length + 1);
        }

        const sum = counts.reduce(add, 0);
        const sum2 = sum / 2;   // sum2 is the half of the total count

        const tr = 'translate(' + (chartWidth / 2) + "," + (chartHeight / 2) + ')';
        const drawing = this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(pie(counts))
            .enter()
            .append("g")
            .attr('transform', tr)
            .append("path")
            .attr("d", arc)
            .attr('fill', (d,i) => this.color(i, counts.length))
            .append("svg:title")
            .text((d,i) => this.label(i) + ":" + this.countAsString(counts[i], confidence[i], sum))
            .exit();

        let total = 0;
        // labels to the left and right of the pie
        const left = [];
        const right = [];
        // ideal position of each label as a value between 0 and 1 (relative angle from the top).
        const leftPosition: ValueAndIndex[] = [];
        const rightPosition: ValueAndIndex[] = [];
        for (let i = 0; i < counts.length; i++) {
            const label = this.label(i);
            const c = counts[i];
            if (c == 0)
                continue;
            if (total + c/2 < sum2) {
                right.push(label);
                const pos = (total + c/2) / sum2;
                rightPosition.push({ value: pos, index: i});
            } else {
                left.push(label);
                const pos = (2 - (total + c/2) / sum2);
                leftPosition.push({ value: pos, index: i});
            }
            total += c;
        }
        // leftPosition and rightPosition are the ideal positions of the labels on the y axis
        // However, we may be unable to fit the labels at these positions, so we now adjust them
        // based on how many neighbors they have
        const rightLabelPosition: ValueAndIndex[] = [];
        const leftLabelPosition: ValueAndIndex[] = [];
        const spacingSize = Math.min(chartHeight / Math.max(rightPosition.length, leftPosition.length), 12);
        // We go from top to bottom and assign label positions on the right side
        let previousPosition = 0;
        for (let i = 0; i < rightPosition.length; i++) {
            const min = Math.max(i * spacingSize, previousPosition + spacingSize);
            const max = chartHeight - (rightPosition.length - i - 1) * spacingSize; // -1 for the start of the next label
            let ideal = chartHeight / 2 - Math.cos(rightPosition[i].value * Math.PI) * radius;
            let labelPosition = PiePlot.clamp(ideal, min, max);
            rightLabelPosition.push({ value: labelPosition, index: rightPosition[i].index });
            previousPosition = labelPosition;
        }
        // We go from bottom to top and assign label positions on the left side
        previousPosition = chartHeight;
        for (let i = 0; i < leftPosition.length; i++) {
            const min = (leftPosition.length - i) * spacingSize;
            const max = Math.min(previousPosition - spacingSize, chartHeight - (i + 1) * spacingSize);
            const ideal = chartHeight / 2 - Math.cos(leftPosition[i].value * Math.PI) * radius;
            let labelPosition = PiePlot.clamp(ideal, min, max);
            leftLabelPosition.push({ value: labelPosition, index: leftPosition[i].index });
            previousPosition = labelPosition;
        }

        const eRad = radius * .9; // endpoint of line is .9 of the radius
        const textSpacing = 5; // space between label and line
        const fontSize = Math.min(spacingSize, 12);
        // labels for pie segments
        drawing.data(leftLabelPosition)
            .enter()
            .append("text")
            .attr("x", chartWidth / 2 - radius * 1.1)
            .attr("y", (d) => d.value)
            .attr("font-size", fontSize)
            .attr("text-anchor", "end")
            .text((d) => this.label(d.index))
            .append("svg:title")
            .text((d) => this.countAsString(counts[d.index], confidence[d.index], sum));
        drawing.data(rightLabelPosition)
            .enter()
            .append("text")
            .attr("x", chartWidth / 2 + radius * 1.1)
            .attr("y", (d) => d.value)
            .attr("font-size", fontSize)
            .attr("text-anchor", "start")
            .text((d) => this.label(d.index))
            .append("svg:title")
            .text((d) => this.countAsString(counts[d.index], confidence[d.index], sum));
        // lines connecting labels to pie segments
        drawing.data(rightLabelPosition)
            .enter()
            .append("line")
            .attr("x1", chartWidth / 2 + radius * 1.1 - textSpacing)
            .attr("y1", (d) => d.value - fontSize / 2)
            .attr("x2", (d, i) => chartWidth / 2 + eRad * Math.sin(rightPosition[i].value * Math.PI))
            .attr("y2", (d, i) => chartHeight / 2 - eRad * Math.cos(rightPosition[i].value * Math.PI))
            .attr("style", "stroke:black");
        drawing.data(leftLabelPosition)
            .enter()
            .append("line")
            .attr("x1", chartWidth / 2 - radius * 1.1 + textSpacing)
            .attr("y1", (d) => d.value - fontSize / 2)
            .attr("x2", (d, i) => chartWidth / 2 - eRad * Math.sin(leftPosition[i].value * Math.PI))
            .attr("y2", (d, i) => chartHeight / 2 - eRad * Math.cos(leftPosition[i].value * Math.PI))
            .attr("style", "stroke:black");
    }

    public draw(): void {
        if (this.data == null)
            return;

        this.drawPie();
    }
}
