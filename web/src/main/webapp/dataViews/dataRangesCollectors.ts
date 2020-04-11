/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import {OnCompleteReceiver} from "../rpc";
import {BucketsInfo, HistogramArgs, IColumnDescription, kindIsString, RemoteObjectId,} from "../javaBridge";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {FullPage, PageTitle} from "../ui/fullPage";
import {ICancellable, periodicSamples, Seed} from "../util";
import {SchemaClass} from "../schemaClass";
import {ChartOptions, Resolution, Size} from "../ui/ui";
import {PlottingSurface} from "../ui/plottingSurface";
import {TrellisHistogramReceiver} from "./trellisHistogramView";
import {HeatmapReceiver} from "./heatmapView";
import {Histogram2DReceiver} from "./histogram2DView";
import {HistogramReceiver} from "./histogramView";
import {AxisData} from "./axisData";
import {TrellisHeatmapReceiver} from "./trellisHeatmapView";
import {TrellisHistogram2DReceiver} from "./trellisHistogram2DView";
import {DatasetView} from "../datasetView";
import {QuartilesHistogramReceiver} from "./quartilesVectorView";

/**
 * Describes the shape of trellis display.
 */
export interface TrellisShape {
    /** The number of plots in a row. */
    xNum: number;
    /** The number of plots in a column. */
    yNum: number;
    /** The size of the header in pixels. */
    headerHeight: number;
    // The size of a plot in pixels, excluding the header.
    size: Size;
    /** The fraction of available display used by the trellis display. This is the
     * parameter that our algorithm optimizes, subject to constraints on the aspect ratio and minimum
     * width and height of each histogram. This should be a fraction between 0 and 1. A value larger
     * than 1 indicates that there is no feasible solution.
     */
    coverage: number;
    /** The actual number of windows to display. */
    bucketCount: number;
}

export class TrellisLayoutComputation {
    protected maxWidth: number;
    protected maxHeight: number;

    /**
     * Optimizes the shape of a trellis display.
     * @param xMax: The width of the display in pixels.
     * @param xMin: Minimum width of a single histogram in pixels.
     * @param yMax: The height of the display in pixels.
     * @param yMin: Minimum height of a single histogram in pixels.
     * @param maxRatio: The maximum aspect ratio we want in our histograms.
     * @param headerHt: The header height for each window.
     *                   x_min and y_min should satisfy the aspect ratio condition:
     *                   Max(x_min/y_min, y_min/x_min) <= max_ratio.
     */
    public constructor(public xMax: number,
                       public xMin: number,
                       public yMax: number,
                       public yMin: number,
                       public headerHt: number,
                       public maxRatio: number) {
        this.maxWidth = Math.floor(xMax / xMin);
        this.maxHeight = Math.floor(yMax / (yMin + headerHt));
        console.assert(Math.max(xMin / yMin, yMin / xMin) <= maxRatio,
            "The minimum sizes do not satisfy aspect ratio");
    }

    public static resize(xMax: number, yMax: number, shape: TrellisShape): TrellisShape {
        return {
            ...shape, size: {
                width: Math.floor(xMax / shape.xNum),
                height: Math.floor(yMax / shape.yNum) - shape.headerHeight
            }
        };
    }

    public getShape(nBuckets: number): TrellisShape {
        const total = this.xMax * this.yMax;
        let used = nBuckets * this.xMin * (this.yMin + this.headerHt);
        let coverage = used / total;
        const opt: TrellisShape = {
            xNum: this.maxWidth,
            yNum: this.maxHeight,
            bucketCount: nBuckets,
            size: {width: this.xMin, height: this.yMin},
            coverage: coverage,
            headerHeight: this.headerHt
        };
        if (this.maxWidth * this.maxHeight < nBuckets) {
            return opt;
        }
        const sizes: number[][] = [];
        for (let i = 1; i <= this.maxWidth; i++)
            for (let j = 1; j <= this.maxHeight; j++)
                if (i * j >= nBuckets)
                    sizes.push([i, j]);
        let xLen: number;
        let yLen: number;
        for (const size of sizes) {
            xLen = Math.floor(this.xMax / size[0]);
            yLen = Math.floor(this.yMax / size[1]) - this.headerHt;
            if (xLen >= yLen)
                xLen = Math.min(xLen, this.maxRatio * (yLen + this.headerHt));
            else
                yLen = Math.min(yLen, this.maxRatio * xLen);
            used = nBuckets * xLen * yLen;
            coverage = used / total;
            if ((xLen >= this.xMin) && (yLen >= this.yMin) && (coverage > opt.coverage)) {
                opt.xNum = size[0];
                opt.yNum = size[1];
                opt.size = {width: xLen, height: yLen};
                opt.coverage = coverage;
                opt.bucketCount = opt.xNum * opt.yNum;
            }
        }
        return opt;
    }
}

/**
 * Waits for 2 or 3 column stats to be received and then
 * initiates a 2D histogram, heatmap, or Trellis plot rendering.
 */
export class DataRangesReceiver extends OnCompleteReceiver<BucketsInfo[]> {
    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<BucketsInfo[]>,
        protected schema: SchemaClass,
        protected bucketCounts: number[], // if 0 we get to choose
        protected cds: IColumnDescription[],
        protected title: PageTitle,
        protected options: ChartOptions) {
        super(page, operation, "histogram");
    }

    protected isPrivate(): boolean {
        return this.page.dataset.isPrivate();
    }

    public static samplingRate(bucketCount: number, rowCount: number, chartSize: Size): number {
        const constant = 4;  // This models the confidence we want from the sampling
        const height = chartSize.height;
        const sampleCount = constant * height * height;
        const sampleRate = sampleCount / rowCount;
        return Math.min(sampleRate, 1);
    }

    /**
     * Compute the parameters to use for a histogram
     * @param cd           Column to compute histogram for.
     * @param range        Range of the data in the column.
     * @param bucketCount  Desired number of buckets; if 0 it will be computed.
     * @param exact        If true we don't sample.
     * @param chartSize    Size available to draw the histogram.
     */
    public static computeHistogramArgs(
        cd: IColumnDescription,
        range: BucketsInfo,
        bucketCount: number,
        exact: boolean,
        chartSize: Size): HistogramArgs {
        if (kindIsString(cd.kind)) {
            const cdfBucketCount = range.stringQuantiles.length;
            let samplingRate = DataRangesReceiver.samplingRate(
                cdfBucketCount, range.presentCount, chartSize);
            if (exact)
                samplingRate = 1.0;
            let bounds = range.stringQuantiles;
            if (bucketCount !== 0)
                bounds = periodicSamples(range.stringQuantiles, bucketCount);
            return {
                cd: cd,
                seed: Seed.instance.getSampled(samplingRate),
                samplingRate: samplingRate,
                leftBoundaries: bounds,
                bucketCount: bounds.length,
            };
        } else {
            let cdfCount = Math.floor(chartSize.width);
            if (bucketCount !== 0)
                cdfCount = bucketCount;

            let adjust = 0;
            if (cd.kind === "Integer") {
                if (cdfCount > range.max - range.min)
                    cdfCount = range.max - range.min + 1;
                adjust = .5;
            }

            let samplingRate = 1.0;
            if (!exact)
                samplingRate = DataRangesReceiver.samplingRate(
                    cdfCount, range.presentCount, chartSize);
            // noinspection UnnecessaryLocalVariableJS
            const args: HistogramArgs = {
                cd: cd,
                min: range.min - adjust,
                max: range.max + adjust,
                samplingRate: samplingRate,
                seed: Seed.instance.getSampled(samplingRate),
                bucketCount: cdfCount,
            };
            return args;
        }
    }

    private trellisLayout(windows: number): TrellisShape {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const tlc = new TrellisLayoutComputation(
            chartSize.width, Resolution.minTrellisWindowSize,
            chartSize.height, Resolution.minTrellisWindowSize,
            Resolution.lineHeight, 1.2);
        return tlc.getShape(windows);
    }

    public run(ranges: BucketsInfo[]): void {
        for (const range of ranges) {
            if (range == null) {
                console.log("Null range received");
                return;
            }
            if (range.presentCount === 0) {
                this.page.reportError("No non-missing data");
                return;
            }
        }
        const rowCount = ranges[0].presentCount + ranges[0].missingCount;
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());

        // variables when drawing Trellis plots
        let trellisShape: TrellisShape = null;
        let windows: number = null;  // number of Trellis windows
        if (this.options.chartKind === "TrellisHeatmap" ||
            this.options.chartKind === "TrellisHistogram" ||
            this.options.chartKind === "Trellis2DHistogram") {
            const groupByIndex = ranges.length === 3 ? 2 : 1;
            if (this.bucketCounts[groupByIndex] !== 0) {
                windows = this.bucketCounts[groupByIndex];
            } else {
                const maxWindows =
                    Math.floor(chartSize.width / Resolution.minTrellisWindowSize) *
                    Math.floor(chartSize.height / Resolution.minTrellisWindowSize);
                if (kindIsString(this.cds[groupByIndex].kind))
                    windows = Math.min(maxWindows, ranges[groupByIndex].stringQuantiles.length);
                else if (this.cds[groupByIndex].kind === "Integer")
                    windows = Math.min(maxWindows,
                        ranges[groupByIndex].max - ranges[groupByIndex].min + 1);
                else
                    windows = maxWindows;
            }
            trellisShape = this.trellisLayout(windows);
        }

        switch (this.options.chartKind) {
            case "QuartileVector": {
                if (ranges[0].presentCount === 0) {
                    this.page.reportError("All values are missing");
                    return;
                }

                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.min(
                        Math.floor(chartSize.width / Resolution.minBarWidth),
                        Resolution.maxBucketCount);
                }

                const histoArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    this.options.exact, chartSize);
                const rr = this.originator.createHistogramRequest(histoArg);
                rr.chain(this.operation);
                const axisData = new AxisData(this.cds[0], ranges[0], histoArg.bucketCount);
                const rec = new QuartilesHistogramReceiver(this.title, this.page,
                    this.originator.remoteObjectId, rowCount, this.schema,
                    this.cds[1],
                    histoArg.bucketCount,
                    axisData, rr, this.options.reusePage);
                rr.invoke(rec);
                break;
            }
            case "Histogram": {
                if (ranges[0].presentCount === 0) {
                    this.page.reportError("All values are missing");
                    return;
                }

                const args: HistogramArgs[] = [];
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.min(
                        Math.floor(chartSize.width / Resolution.minBarWidth),
                        Resolution.maxBucketCount);
                }

                const histoArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    this.options.exact || this.isPrivate(), chartSize);
                args.push(histoArg);
                const cdfArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], 0,
                    this.options.exact|| this.isPrivate(), chartSize);
                args.push(cdfArg);

                const rr = this.originator.createHistogramAndCDFRequest(args);
                rr.chain(this.operation);
                const axisData = new AxisData(this.cds[0], ranges[0], histoArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Histogram of " + this.schema.displayName(this.cds[0].name).toString());
                const renderer = new HistogramReceiver(this.title, this.page,
                    this.originator.remoteObjectId, rowCount, this.schema, axisData, rr, cdfArg.samplingRate, this.options.pieChart, this.options.reusePage); // TODO sampling rate?
                rr.invoke(renderer);
                break;
            }
            case "TrellisHistogram": {
                console.assert(ranges.length === 2);
                // noinspection JSObjectNullOrUndefined
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], this.bucketCounts[0],
                    this.options.exact || this.isPrivate(), trellisShape.size);
                const groups = this.bucketCounts[1] === 0 ? trellisShape.bucketCount : this.bucketCounts[1];
                const wArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1], groups, this.options.exact || this.isPrivate(), trellisShape.size);
                // Window argument comes first
                const args = [wArg, xArg];
                // Trellis histograms are computed by heatmap requests
                const rr = this.originator.createHeatmapRequest(args);
                const xAxisData = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[1], ranges[1], wArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Histograms of " + this.schema.displayName(this.cds[0].name).toString() +
                        " grouped by " + this.schema.displayName(this.cds[1].name).toString());
                const renderer = new TrellisHistogramReceiver(this.title, this.page,
                    this.originator, rowCount, this.schema,
                    [xAxisData, groupByAxis], this.bucketCounts[0],
                    xArg.samplingRate, trellisShape, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Trellis2DHistogram": {
                const args: HistogramArgs[] = [];
                // noinspection JSObjectNullOrUndefined
                const maxXBucketCount = Math.floor(trellisShape.size.width / Resolution.minBarWidth);
                const maxYBucketCount = Resolution.maxBucketCount;
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact || this.isPrivate(), trellisShape.size);
                const yArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact || this.isPrivate(), trellisShape.size);
                const wArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[2], ranges[2], trellisShape.bucketCount, this.options.exact || this.isPrivate(), chartSize);
                // Window argument comes first
                args.push(wArg);
                args.push(xArg);
                args.push(yArg);
                const rr = this.originator.createTrellis2DHistogramRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1], yArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[2], ranges[2], wArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                                 "Histograms (" + this.schema.displayName(this.cds[0].name).displayName +
                                 ", " + this.schema.displayName(this.cds[1].name).displayName +
                                 ") grouped by " + this.schema.displayName(this.cds[2].name).displayName);
                const renderer = new TrellisHistogram2DReceiver(this.title, this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yAxis, groupByAxis], 1.0, trellisShape, rr, this.options);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "TrellisHeatmap": {
                const args: HistogramArgs[] = [];
                // noinspection JSObjectNullOrUndefined
                const maxXBucketCount = Math.floor(trellisShape.size.width / Resolution.minDotSize);
                const maxYBucketCount = Math.floor(trellisShape.size.height / Resolution.minDotSize);
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact || this.isPrivate(), trellisShape.size);
                const yArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact || this.isPrivate(), trellisShape.size);
                const wArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[2], ranges[2], trellisShape.bucketCount, this.options.exact || this.isPrivate(), chartSize);
                // Window argument comes first
                args.push(wArg);
                args.push(xArg);
                args.push(yArg);

                const rr = this.originator.createHeatmap3DRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1], yArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[2], ranges[2], wArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Heatmaps (" + this.schema.displayName(this.cds[0].name).displayName +
                        ", " + this.schema.displayName(this.cds[1].name).displayName +
                        ") grouped by " + this.schema.displayName(this.cds[2].name).displayName);
                const renderer = new TrellisHeatmapReceiver(this.title, this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yAxis, groupByAxis], 1.0, trellisShape, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Heatmap": {
                const args: HistogramArgs[] = [];
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0)
                    maxXBucketCount = Math.floor(chartSize.width / Resolution.minDotSize);
                let maxYBucketCount = this.bucketCounts[1];
                if (maxYBucketCount === 0)
                    maxYBucketCount = Math.floor(chartSize.height / Resolution.minDotSize);
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact || this.isPrivate(), chartSize);
                args.push(xArg);
                const yArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact || this.isPrivate(), chartSize);
                args.push(yArg);

                const rr = this.originator.createHeatmapRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1], yArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Heatmap (" + this.schema.displayName(this.cds[0].name).displayName + ", " +
                        this.schema.displayName(this.cds[1].name).displayName + ")");
                const renderer = new HeatmapReceiver(this.title, this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yAxis], 1.0, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "2DHistogram": {
                const args: HistogramArgs[] = [];
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.min(
                        Math.floor(chartSize.width / Resolution.minBarWidth),
                        Resolution.maxBucketCount);
                }
                const maxYBucketCount = Resolution.maxBucketCount;

                // The first two represent the resolution for the 2D histogram
                const xarg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    // Relative views cannot sample
                    this.options.exact || this.options.relative || this.isPrivate(), chartSize);
                args.push(xarg);
                const yarg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount,
                    this.options.exact || this.options.relative || this.isPrivate(), chartSize);
                args.push(yarg);
                // This last one represents the resolution for the CDF
                const cdfArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], 0,
                    this.options.exact || this.isPrivate(), chartSize);
                args.push(cdfArg);
                const rr = this.originator.createHistogram2DRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xarg.bucketCount);
                const yData = new AxisData(this.cds[1], ranges[1], yarg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Histogram (" + this.schema.displayName(this.cds[0].name).displayName + ", " +
                    this.schema.displayName(this.cds[1].name).displayName + ")");
                const renderer = new Histogram2DReceiver(this.title, this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yData], cdfArg.samplingRate, rr,
                    this.options);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            default:
                console.assert(false);
                break;
        }
    }
}

/**
 * Receives the result of a filtering operation and initiates
 * a new range computation, which in turns initiates a chart
 * rendering.
 */
export class FilterReceiver extends BaseReceiver {
    constructor(protected title: PageTitle,
                protected cds: IColumnDescription[],
                protected schema: SchemaClass,
                protected bucketCounts: number[],
                page: FullPage,
                operation: ICancellable<RemoteObjectId>,
                dataset: DatasetView,
                protected options: ChartOptions) {
        super(page, operation, "Filter", dataset);
    }

    public run(): void {
        super.run();
        const rr = this.remoteObject.createDataQuantilesRequest(this.cds, this.page, this.options.chartKind);
        rr.invoke(new DataRangesReceiver(this.remoteObject, this.page, rr, this.schema,
                  this.bucketCounts, this.cds, this.title, this.options));
    }
}
