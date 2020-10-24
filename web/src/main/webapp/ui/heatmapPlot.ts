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

import {AxisData, AxisKind} from "../dataViews/axisData";
import {Groups, kindIsString, RowValue} from "../javaBridge";
import {Color, ColorMap, regression, Triple, valueWithConfidence} from "../util";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {SchemaClass} from "../schemaClass";
import {D3SvgElement, Resolution} from "./ui";

interface Dot {
    x: number;
    y: number;
    count: number;
    valueIndex: number;  // index in detailColorMap
    confRatio: number;  // can be negative.  Large values (> 2) -> confident.  Small values: not confident.
}

export class HeatmapPlot
    extends Plot<Triple<Groups<Groups<number>>, Groups<Groups<number>> | null, Groups<Groups<RowValue[]>> | null>> {
    protected pointWidth: number; // in pixels
    protected pointHeight: number; // in pixels
    protected max: number;  // maximum count
    protected visible: number;
    protected distinct: number;
    protected dots: Dot[];
    protected schema: SchemaClass;
    protected isPrivate: boolean;
    protected showRegression: boolean;
    protected xPoints: number;
    protected yPoints: number;
    protected regressionLine: D3SvgElement;

    public constructor(surface: PlottingSurface,
                       protected colorMap: ColorMap,
                       protected detailColorMap: ColorMap | null,
                       protected detailIndex: number,
                       protected showAxes: boolean) {
        super(surface);
        this.dots = [];
        this.isPrivate = false;
        this.showRegression = true;
        this.regressionLine = null;
    }

    public toggleRegression(): void {
        this.showRegression = !this.showRegression;
        if (this.regressionLine != null) {
            if (this.showRegression)
                this.regressionLine.attr("stroke-width", 1);
            else
                this.regressionLine.attr("stroke-width", 0);
        }
    }

    public draw(): void {
        if (this.dots == null) {
            this.plottingSurface.reportError("No data to display");
            return;
        }

        const canvas = this.plottingSurface.getCanvas();
        if (this.showAxes) {
            canvas.append("text")
                .text(this.yAxisData.getName())
                .attr("dominant-baseline", "text-before-edge");
            canvas.append("text")
                .text(this.xAxisData.getName())
                .attr("x", this.getChartWidth() / 2)
                .attr("y", this.getChartHeight() + this.plottingSurface.topMargin +
                        this.plottingSurface.bottomMargin / 2)
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "hanging");
        }

        const htmlCanvas: HTMLCanvasElement = document.createElement("canvas");
        htmlCanvas.height = this.getChartHeight();
        htmlCanvas.width = this.getChartWidth();
        // draw the image onto the canvas.
        const ctx: CanvasRenderingContext2D = htmlCanvas.getContext("2d")!;
        for (const dot of this.dots) {
            ctx.beginPath();
            let color: Color | null;
            if (this.detailColorMap != null && dot.count === 1)
                color = Color.parse(this.detailColorMap(dot.valueIndex));
            else if (dot.count <= 0) // can happen with privacy noise added
                color = new Color(.9, .9, .9);
            else
                color = Color.parse(this.colorMap(dot.count));
            color = color!.brighten(1 / dot.confRatio);
            ctx.fillStyle = color.toString();
            ctx.fillRect(dot.x, dot.y, this.pointWidth, this.pointHeight);
            ctx.closePath();
        }
        const url = htmlCanvas.toDataURL("image/png");
        this.plottingSurface.getChart()
            .append("image")
            .attr("xlink:href", url)
            .attr("width", this.getChartWidth())
            .attr("height", this.getChartHeight());
        if (this.showAxes)
            this.drawAxes();

        if (this.xAxisData != null &&
            this.yAxisData != null &&
            !kindIsString(this.yAxisData.description!.kind) &&
            !kindIsString(this.xAxisData.description!.kind) &&
            !this.isPrivate &&
            this.showRegression) {
            // It makes no sense to do regressions for string values.
            // Regressions for private data should be computed in a different way; this
            // way gives too much noise.
            const regr = regression(this.data.first.perBucket.map(l => l.perBucket));
            if (regr.length === 2) {
                const b = regr[0];
                const a = regr[1];
                const y1 = this.getChartHeight() - (b + .5) * this.pointHeight;
                const y2 = this.getChartHeight() - (a * this.xPoints + b + .5) * this.pointHeight;
                this.regressionLine = this.plottingSurface.getChart()
                    .append("line")
                    .attr("x1", 0)
                    .attr("y1", y1)
                    .attr("x2", this.pointWidth * this.xPoints)
                    .attr("y2", y2)
                    .attr("stroke", "black");
            }
        }
    }

    public getBucketIndex(x: number, y: number): [number, number] | null {
        let xi = x / this.pointWidth;
        let yi = (this.getChartHeight() - y) / this.pointHeight;
        xi = Math.floor(xi);
        yi = Math.floor(yi);
        if (xi >= 0 && xi < this.xPoints && yi >= 0 && yi < this.yPoints) {
            return [xi, yi];
        }
        return null;
    }

    /**
     * Given two screen coordinates within the chart, return the count displayed at that coordinate.
     */
    public getCount(x: number, y: number): [number, number] {
        const c = this.getBucketIndex(x, y);
        if (c == null)
            return valueWithConfidence(0, null);
        const [xi, yi] = c;
        const value = this.data.first.perBucket[xi].perBucket[yi];
        const conf = this.data.second != null ?
            this.data.second.perBucket[xi].perBucket[yi] : null;
        return valueWithConfidence(value, conf);
    }

    public getMaxCount(): number {
        return this.max;
    }

    public getVisiblePoints(): number {
        return this.visible;
    }

    public getDistinct(): number {
        return this.distinct;
    }

    public setData(heatmap: Triple<Groups<Groups<number>>, Groups<Groups<number>> | null, Groups<Groups<RowValue[]>> | null>,
                   xData: AxisData, yData: AxisData, detailData: AxisData | null,
                   schema: SchemaClass, isPrivate: boolean): void {
        this.data = heatmap;
        this.xAxisData = xData;
        this.yAxisData = yData;
        this.schema = schema;
        this.isPrivate = isPrivate;
        if (this.xAxisData != null)
            this.xAxisData.setResolution(
                this.getChartWidth(), AxisKind.Bottom, PlottingSurface.bottomMargin);
        if (this.yAxisData != null)
            this.yAxisData.setResolution(
                this.getChartHeight(), AxisKind.Left, Resolution.heatmapLabelWidth);

        this.xPoints = this.data.first.perBucket.length;
        this.yPoints = this.data.first.perBucket[0].perBucket.length;
        if (this.xPoints === 0 || this.yPoints === 0)
            return;
        this.pointWidth = this.getChartWidth() / this.xPoints;
        this.pointHeight = this.getChartHeight() / this.yPoints;

        this.max = 0;
        this.visible = 0;
        this.distinct = 0;
        this.dots = [];

        for (let x = 0; x < this.xPoints; x++) {
            for (let y = 0; y < this.yPoints; y++) {
                const b = this.data.first.perBucket[x].perBucket[y];
                const count = Math.max(0, b);
                let confRatio: number;
                let valueIndex = 0;
                if (!isPrivate) {
                    confRatio = 1;
                } else {
                    let confidence = this.data.second!.perBucket[x].perBucket[y];
                    if (confidence <= 1) // should never happen
                        confRatio = count;
                    else
                        confRatio = Math.max(count, 1) / confidence;
                }
                if (count === 1 && this.data.third != null && detailData != null) { // could happen if we have only 2 cols.
                    const value = this.data.third.perBucket[x].perBucket[y][this.detailIndex];
                    valueIndex = detailData.bucketIndex(value);
                }
                if (count > this.max)
                    this.max = count;
                if (this.isPrivate || (!this.isPrivate && count !== 0)) {
                    const rec: Dot = {
                        x: x * this.pointWidth,
                        // +1 because it's the upper corner
                        y: this.getChartHeight() - (y + 1) * this.pointHeight,
                        count,
                        valueIndex,
                        confRatio
                    };
                    this.visible += count;
                    this.distinct++;
                    this.dots.push(rec);
                }
            }
        }
    }
}
