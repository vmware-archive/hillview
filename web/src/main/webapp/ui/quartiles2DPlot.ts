/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import {axisLeft as d3axisLeft} from "d3-axis";
import {format as d3format} from "d3-format";
import {scaleLinear as d3scaleLinear} from "d3-scale";
import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {BucketsInfo, Groups, SampleSet} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale} from "./ui";
import {SchemaClass} from "../schemaClass";

/**
 * A QuartilesPlot draws a vector if whisker plots
 * on a PlottingSurface, including the axes.
 */
export class Quartiles2DPlot extends Plot {
    public qv: Groups<SampleSet>;
    /**
     * Sampling rate that was used to compute the histogram.
     */
    public samplingRate: number;
    public barWidth: number;
    protected yScale: D3Scale;
    protected yAxis: D3Axis;
    public max: number;  // maximum quantile value
    public min: number;
    protected missingYCount: number;
    protected rowCount: number;
    public displayAxes: boolean;
    public isPrivate: boolean;
    protected schema: SchemaClass;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
        this.displayAxes = true;
    }

    /**
     * Set the histogram that we want to draw.
     * @param qv            Data to plot.
     * @param samplingRate  Sampling rate used to compute this view.
     * @param schema        Table schema.
     * @param rowCount      Total number of rows in dataset.
     * @param axisData      Description of the X axis.
     * @param isPrivate     True if we are plotting private data.
     */
    public setData(qv: Groups<SampleSet>, samplingRate: number,
                   schema: SchemaClass, rowCount: number,
                   axisData: AxisData, isPrivate: boolean): void {
        this.qv = qv;
        this.rowCount = rowCount;
        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        this.schema = schema;
        this.isPrivate = isPrivate;
    }

    private drawBars(): void {
        if (this.qv == null || this.qv.perBucket == null)
            return;
        const bucketCount = this.qv.perBucket.length;
        const maxes = this.qv.perBucket.map(v => v.max);
        const mins = this.qv.perBucket.map(v => v.min);
        this.max = Math.max(...maxes);
        this.min = Math.min(...mins);
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        const yScale = chartHeight / (this.max - this.min);
        this.xAxisData.setResolution(chartWidth, AxisKind.Bottom, PlottingSurface.bottomMargin);
        this.yScale = d3scaleLinear()
            .range([chartHeight, 0])
            .domain([this.min, this.max]);
        this.yAxis = d3axisLeft(this.yScale).tickFormat(d3format(".2s"));
        this.barWidth = chartWidth / bucketCount;

        this.plottingSurface.getCanvas().append("text")
            .text(this.xAxisData.getDisplayNameString(this.schema))
            .attr("transform", `translate(${this.getChartWidth() / 2},
            ${this.getChartHeight() + this.plottingSurface.topMargin + this.plottingSurface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        const whiskers = [];
        this.missingYCount = 0;
        for (let x = 0; x < this.qv.perBucket.length; x++) {
            const q = this.qv.perBucket[x];
            this.missingYCount += q.missing;
            if (q.count === 0)
                continue;
            // If we do not have enough samples replicate the existing ones
            const extra = q.samples;
            for (let i = q.samples.length; i < 3; i++)
                extra.push(q.samples[q.samples.length - 1]);
            const whisker = {
                x: x,
                min: q.min,
                max: q.max,
                q1: extra[0],
                q2: extra[1],
                q3: extra[2]
            };
            whiskers.push(whisker);
        }

        const yPos = d => chartHeight - (d - this.min) * yScale;
        const xL = d => (d + .5) * this.barWidth - lineWidth / 2;
        const xR = d => xL(d) + lineWidth;

        const lineWidth = this.barWidth * 3/4;
        const support = this.plottingSurface.getChart()
            .selectAll("g")
            .data(whiskers)
            .enter()
            .append("g");
        support.append("rect")
            .attr("x", (l) => xL(l.x))
            .attr("y", (l) => yPos(l.q3))
            .attr("height", (l) => yPos(l.q1) - yPos(l.q3))
            .attr("width", lineWidth)
            .attr("stroke-width", 1)
            .attr("stroke", "black")
            .attr("fill", "darkcyan");
        support.append("svg:line")
            .attr("x1", (l) => xL(l.x) + lineWidth / 2)
            .attr("y1", (l) => yPos(l.max))
            .attr("x2", (l) => xL(l.x) + lineWidth / 2)
            .attr("y2", (l) => yPos(l.q3))
            .attr("stroke-width", 1)
            .attr("stroke", "black");
        support.append("svg:line")
            .attr("x1", (l) => xL(l.x) + lineWidth / 2)
            .attr("y1", (l) => yPos(l.q1))
            .attr("x2", (l) => xL(l.x) + lineWidth / 2)
            .attr("y2", (l) => yPos(l.min))
            .attr("stroke-width", 1)
            .attr("stroke", "black");
        support.append("svg:line")
            .attr("x1", (l) => xL(l.x))
            .attr("x2", (l) => xR(l.x))
            .attr("y1", (l) => yPos(l.min))
            .attr("y2", (l) => yPos(l.min))
            .attr("stroke-width", 1)
            .attr("stroke", "black")
            .attr("stroke-linecap", "round");
        support.append("svg:line")
            .attr("x1", (l) => xL(l.x))
            .attr("x2", (l) => xR(l.x))
            .attr("y1", (l) => yPos(l.max))
            .attr("y2", (l) => yPos(l.max))
            .attr("stroke-width", 1)
            .attr("stroke", "black")
            .attr("stroke-linecap", "round");
        support.append("svg:line")
            .attr("x1", (l) => xL(l.x))
            .attr("x2", (l) => xR(l.x))
            .attr("y1", (l) => yPos(l.q2))
            .attr("y2", (l) => yPos(l.q2))
            .attr("stroke-width", 1)
            .attr("stroke", "black")
            .attr("stroke-linecap", "round");
    }

    private yLabel(value: number): number {
        return this.yScale(value);
    }

    public draw(): void {
        if (this.qv == null)
            return;

        this.drawBars();
        if (this.displayAxes)
            this.drawAxes();
    }

    public getYAxis(): AxisDescription {
        return new AxisDescription(this.yAxis, 1, false, null);
    }

    public getYScale(): D3Scale {
        return this.yScale;
    }

    /**
     * The index of the bucket covering the current x position on the X axis.
     */
    public getBucketIndex(x: number): number {
        const bucket = Math.floor(x / this.barWidth);
        if (bucket < 0 || this.qv == null ||
            bucket >= this.qv.perBucket.length)
            return -1;
        return bucket;
    }

    public yDataRange(): BucketsInfo {
        return {
            min: this.min,
            max: this.max,
            missingCount: this.missingYCount,
            presentCount: this.rowCount - this.missingYCount
        };
    }
}
