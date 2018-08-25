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
import {DataRange, HistogramArgs, IColumnDescription, kindIsString} from "../javaBridge";
import {TableTargetAPI} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {ICancellable, periodicSamples, Seed} from "../util";
import {SchemaClass} from "../schemaClass";
import {ChartOptions, HistogramOptions, Resolution, Size} from "../ui/ui";
import {PlottingSurface} from "../ui/plottingSurface";
import {TrellisHistogramRenderer} from "./trellisHistogramView";
import {HeatmapRenderer} from "./heatmapView";
import {Histogram2DRenderer} from "./histogram2DView";
import {HistogramRenderer} from "./histogramView";
import {AxisData} from "./axisData";
import {TrellisHeatmapRenderer} from "./trellisHeatmapView";
import {TrellisHistogram2DRenderer} from "./trellisHistogram2DView";

/**
 * Describes the shape of trellis display.
 */
export interface TrellisShape {
    /// The number of plots in a row.
    xNum: number;
    /// The number of plots in a column.
    yNum: number;
    /// The size of a plot in pixels
    size: Size;
    /// The fraction of available display used by the trellis display. This is the
    /// parameter that our algorithm optimizes, subject to constraints on the aspect ratio and minimum
    /// width and height of each histogram. This should be a fraction between 0 and 1. A value larger
    /// than 1 indicates that there is no feasible solution.
    coverage: number;
    /// The actual number of buckets to request.
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
     *                   x_min and y_min should satisfy the aspect ratio condition:
     *                   Max(x_min/y_min, y_min/x_min) <= max_ratio.
     */
    public constructor(public xMax: number,
                       public xMin: number,
                       public yMax: number,
                       public yMin: number,
                       public maxRatio: number) {
        this.maxWidth = xMax / xMin;
        this.maxHeight = yMax / yMin;
        console.assert(Math.max(xMin / yMin, yMin / xMin) <= maxRatio,
            "The minimum sizes do not satisfy aspect ratio");
    }

    public getShape(nBuckets: number): TrellisShape {
        const total = this.xMax * this.yMax;
        let used = nBuckets * this.xMin * this.yMin;
        let coverage = used / total;
        const opt: TrellisShape = {
            xNum: this.maxWidth,
            yNum: this.maxHeight,
            bucketCount: nBuckets,
            size: {width: this.xMin, height: this.yMin},
            coverage: coverage
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
            yLen = Math.floor(this.yMax / size[1]);
            if (xLen >= yLen)
                xLen = Math.min(xLen, this.maxRatio * yLen);
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
export class DataRangesCollector extends OnCompleteReceiver<DataRange[]> {
    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<DataRange[]>,
        protected schema: SchemaClass,
        protected bucketCounts: number[],
        protected cds: IColumnDescription[],  // if 0 we get to choose
        protected title: string | null,
        protected options: ChartOptions) {
        super(page, operation, "histogram");
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
        range: DataRange,
        bucketCount: number,
        exact: boolean,
        chartSize: Size): HistogramArgs {
        if (kindIsString(cd.kind)) {
            const cdfBucketCount = range.leftBoundaries.length;
            let samplingRate = DataRangesCollector.samplingRate(
                cdfBucketCount, range.presentCount, chartSize);
            if (exact)
                samplingRate = 1.0;
            let bounds = range.leftBoundaries;
            if (bucketCount !== 0)
                bounds = periodicSamples(range.leftBoundaries, bucketCount);
            const args: HistogramArgs = {
                cd: cd,
                seed: Seed.instance.getSampled(samplingRate),
                samplingRate: samplingRate,
                leftBoundaries: bounds,
                bucketCount: bounds.length
            };
            return args;
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
                samplingRate = DataRangesCollector.samplingRate(
                    cdfCount, range.presentCount, chartSize);
            const args: HistogramArgs = {
                cd: cd,
                min: range.min - adjust,
                max: range.max + adjust,
                samplingRate: samplingRate,
                seed: Seed.instance.getSampled(samplingRate),
                bucketCount: cdfCount
            };
            return args;
        }
    }

    private trellisLayout(windows: number): TrellisShape {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const tlc = new TrellisLayoutComputation(
            chartSize.width, Resolution.minTrellisWindowSize,
            chartSize.height, Resolution.minTrellisWindowSize,
            1.2);
        return tlc.getShape(windows);
    }

    public run(ranges: DataRange[]): void {
        for (const range of ranges) {
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
            this.options.chartKind === "TrellisHistogram") {
            const groupByIndex = ranges.length === 3 ? 2 : 1;
            const maxWindows =
                Math.floor(chartSize.width / Resolution.minTrellisWindowSize) *
                Math.floor(chartSize.height / Resolution.minTrellisWindowSize);
            if (kindIsString(this.cds[groupByIndex].kind))
                windows = Math.min(maxWindows, ranges[groupByIndex].leftBoundaries.length);
            else if (this.cds[groupByIndex].kind === "Integer")
                windows = Math.min(maxWindows,
                    ranges[groupByIndex].max - ranges[groupByIndex].min + 1);
            else
                windows = maxWindows;
            trellisShape = this.trellisLayout(windows);
        }

        switch (this.options.chartKind) {
            case "TrellisHistogram": {
                console.assert(ranges.length === 2);
                const xArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], 0, false, trellisShape.size);
                const wArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[1], ranges[1], trellisShape.bucketCount, false, trellisShape.size);
                // Window argument comes first
                const args = [wArg, xArg];
                // Trellis histograms are computed by heatmap requests
                const rr = this.originator.createHeatmapRequest(args);
                const xAxisData = new AxisData(this.cds[0], ranges[0]);
                xAxisData.setBucketCount(this.bucketCounts[0]);
                const groupByAxis = new AxisData(this.cds[1], ranges[1]);
                groupByAxis.setBucketCount(wArg.bucketCount);
                const renderer = new TrellisHistogramRenderer(this.page,
                    this.originator, rowCount, this.schema,
                    [xAxisData, groupByAxis],
                    xArg.samplingRate, trellisShape, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Trellis2DHistogram": {
                const args: HistogramArgs[] = [];
                const maxXBucketCount = Math.floor(trellisShape.size.width / Resolution.minDotSize);
                const maxYBucketCount = Resolution.maxBucketCount;
                const xArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact, trellisShape.size);
                const yArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact, trellisShape.size);
                const wArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[2], ranges[2], trellisShape.bucketCount, this.options.exact, chartSize);
                // Window argument comes first
                args.push(wArg);
                args.push(xArg);
                args.push(yArg);
                /*
                TODO
                const rr = this.originator.createTrellis2DHistogramRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0]);
                xAxis.setBucketCount(xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1]);
                yAxis.setBucketCount(yArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[2], ranges[2]);
                groupByAxis.setBucketCount(wArg.bucketCount);
                const renderer = new TrellisHistogram2DRenderer(this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yAxis, groupByAxis], 1.0, trellisShape, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                 */
                break;
            }
            case "TrellisHeatmap": {
                const args: HistogramArgs[] = [];
                const maxXBucketCount = Math.floor(trellisShape.size.width / Resolution.minDotSize);
                const maxYBucketCount = Math.floor(trellisShape.size.height / Resolution.minDotSize);
                const xArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact, trellisShape.size);
                const yArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact, trellisShape.size);
                const wArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[2], ranges[2], trellisShape.bucketCount, this.options.exact, chartSize);
                // Window argument comes first
                args.push(wArg);
                args.push(xArg);
                args.push(yArg);

                const rr = this.originator.createHeatmap3DRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0]);
                xAxis.setBucketCount(xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1]);
                yAxis.setBucketCount(yArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[2], ranges[2]);
                groupByAxis.setBucketCount(wArg.bucketCount);
                const renderer = new TrellisHeatmapRenderer(this.page,
                    this.originator, rowCount, this.schema,
                    [xAxis, yAxis, groupByAxis], 1.0, trellisShape, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Heatmap": {
                const args: HistogramArgs[] = [];
                const maxXBucketCount = Math.floor(chartSize.width / Resolution.minDotSize);
                const maxYBucketCount = Math.floor(chartSize.height / Resolution.minDotSize);
                const xArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, this.options.exact, chartSize);
                args.push(xArg);
                const yArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount, this.options.exact, chartSize);
                args.push(yArg);

                const rr = this.originator.createHeatmapRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0]);
                xAxis.setBucketCount(xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1]);
                yAxis.setBucketCount(yArg.bucketCount);
                const renderer = new HeatmapRenderer(this.page,
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
                const xarg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    // Relative views cannot sample
                    this.options.exact || this.options.relative, chartSize);
                args.push(xarg);
                const yarg = DataRangesCollector.computeHistogramArgs(
                    this.cds[1], ranges[1], maxYBucketCount,
                    this.options.exact || this.options.relative, chartSize);
                args.push(yarg);
                // This last one represents the resolution for the CDF
                const cdfArg = DataRangesCollector.computeHistogramArgs(
                    this.cds[0], ranges[0], 0,
                    this.options.exact || this.options.relative, chartSize);
                args.push(cdfArg);
                const rr = this.originator.createHistogram2DRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0]);
                const yData = new AxisData(this.cds[1], ranges[1]);
                yData.setBucketCount(yarg.bucketCount);
                const renderer = new Histogram2DRenderer(this.page,
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
 * Waits for column stats to be received and then initiates a histogram rendering.
 */
export class DataRangeCollector extends OnCompleteReceiver<DataRange> {
    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<DataRange>,
        protected schema: SchemaClass,
        protected bucketCount: number,
        protected cd: IColumnDescription,  // title of the resulting display
        protected title: string | null,
        protected bucketsRequested: number,
        protected options: HistogramOptions) {
        super(page, operation, "histogram");
    }

    public run(range: DataRange): void {
        if (range.presentCount === 0) {
            this.page.reportError("All values are missing");
            return;
        }

        const rowCount = range.presentCount + range.missingCount;
        if (this.title == null)
            this.title = "Histogram of " + this.cd.name;
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const args = DataRangesCollector.computeHistogramArgs(
            this.cd, range, 0, // ignore the bucket count; we'll integrate the CDF
            this.options.exact, chartSize);
        const rr = this.originator.createHistogramRequest(args);
        rr.chain(this.operation);
        const axisData = new AxisData(this.cd, range);
        const renderer = new HistogramRenderer(this.title, this.page,
            this.originator.remoteObjectId, rowCount, this.schema, this.bucketCount,
            axisData, rr, args.samplingRate, this.options.reusePage);
        rr.invoke(renderer);
    }
}
