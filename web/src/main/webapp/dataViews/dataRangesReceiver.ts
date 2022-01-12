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
import {
    BucketsInfo, HeatmapRequestInfo, HistogramInfo,
    HistogramRequestInfo,
    IColumnDescription,
    kindIsNumeric,
    kindIsString, QuantilesMatrixInfo, QuantilesVectorInfo,
    RemoteObjectId,
} from "../javaBridge";
import {BaseReceiver, TableTargetAPI} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assertNever, ICancellable, periodicSamples, Seed, zip, assert, optionToBoolean} from "../util";
import {SchemaClass} from "../schemaClass";
import {ChartOptions, Resolution, Size} from "../ui/ui";
import {PlottingSurface} from "../ui/plottingSurface";
import {TrellisHistogramReceiver} from "./trellisHistogramView";
import {HeatmapReceiver, HeatmapWithDataReceiver} from "./heatmapView";
import {Histogram2DReceiver} from "./histogram2DView";
import {HistogramReceiver} from "./histogramView";
import {AxisData} from "./axisData";
import {TrellisHeatmapReceiver} from "./trellisHeatmapView";
import {TrellisHistogram2DReceiver} from "./trellisHistogram2DView";
import {DatasetView} from "../datasetView";
import {QuartilesVectorReceiver} from "./quartilesHistogramView";
import {TrellisHistogramQuartilesReceiver} from "./trellisHistogramQuartilesView";
import {CorrelationHeatmapReceiver} from "./correlationHeatmapView";
import {TableMeta, ReceiverCommonArgs} from "../ui/receiver";

/**
 * Describes the shape of trellis display.
 */
export interface TrellisShape {
    /** The number of plots in a row. */
    xWindows: number;
    /** The number of plots in a column. */
    yWindows: number;
    /** The size of the header in pixels. */
    headerHeight: number;
    // The size of a plot in pixels, excluding the header.
    size: Size;
    /**
     * The fraction of available display used by the trellis display. This is the
     * parameter that our algorithm optimizes, subject to constraints on the aspect ratio and minimum
     * width and height of each histogram. This should be a fraction between 0 and 1. A value larger
     * than 1 indicates that there is no feasible solution.
     */
    coverage: number;
    /**
     * The actual number of windows to display.
     */
    windowCount: number;
    /**
     * If true the last bucket is reserved for missing data.
     */
    missingBucket: boolean;
}

function groupByBuckets(shape: TrellisShape): number {
    return shape.windowCount - (shape.missingBucket ? 1 : 0);
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
                width: Math.floor(xMax / shape.xWindows),
                height: Math.floor(yMax / shape.yWindows) - shape.headerHeight
            }
        };
    }

    public getShape(nBuckets: number, missingData: boolean): TrellisShape {
        const total = this.xMax * this.yMax;
        let used = nBuckets * this.xMin * (this.yMin + this.headerHt);
        let coverage = used / total;
        const opt: TrellisShape = {
            xWindows: this.maxWidth,
            yWindows: this.maxHeight,
            windowCount: nBuckets,
            size: {width: this.xMin, height: this.yMin},
            coverage: coverage,
            headerHeight: this.headerHt,
            missingBucket: missingData
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
                opt.xWindows = size[0];
                opt.yWindows = size[1];
                opt.size = {width: xLen, height: yLen};
                opt.coverage = coverage;
                opt.windowCount = opt.xWindows * opt.yWindows;
            }
        }
        return opt;
    }
}

interface HistogramParameters extends HistogramInfo {
    samplingRate: number;
}

function createRequestArgs(params: HistogramParameters[], sample: boolean): HistogramRequestInfo {
    const samplingRate = sample ?
        params.map(p => p.samplingRate).reduce((a, b) => Math.max(a,b)) :
        1.0;
    const seed = Seed.instance.getSampled(samplingRate);
    return {
        histos: params,
        samplingRate,
        seed
    }
}

/**
 * Waits for 2 or 3 column stats to be received and then
 * initiates a 2D histogram, heatmap, or Trellis plot rendering.
 */
export class DataRangesReceiver extends OnCompleteReceiver<BucketsInfo[]> {
    protected rowCount: number;  // Set from the first range received.

    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<BucketsInfo[]> | null,
        protected readonly meta: TableMeta,
        protected bucketCounts: number[], // if 0 we get to choose
        protected cds: IColumnDescription[],
        protected title: PageTitle | null, // If null we generate a title
        protected provenance: string | null,  // Only used if the title is null
        protected options: ChartOptions) {
        super(page, operation, "histogram");
        console.assert(title != null || provenance != null);
    }

    protected isPrivate(): boolean {
        return this.page.dataset!.isPrivate();
    }

    public static samplingRate(
        bucketCount: number, presentCount: number, missingCount: number, chartSize: Size): number {
        if ((presentCount + missingCount) <= 0)
            return 1.0;
        const constant = 4;  // This models the confidence we want from the sampling
        const height = chartSize.height;
        const sampleCount = constant * height * height;
        const sampleRate = sampleCount / (presentCount + missingCount);
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
        chartSize: Size): HistogramParameters {
        if (kindIsString(cd.kind)) {
            assert(range.stringQuantiles != null);
            const cdfBucketCount = range.stringQuantiles.length;
            let samplingRate = DataRangesReceiver.samplingRate(
                cdfBucketCount, range.presentCount, range.missingCount, chartSize);
            if (exact)
                samplingRate = 1.0;
            let bounds = range.stringQuantiles;
            if (bucketCount !== 0)
                bounds = periodicSamples(range.stringQuantiles, bucketCount)!;
            return {
                cd: cd,
                samplingRate: samplingRate,
                leftBoundaries: bounds,
                bucketCount: bounds.length,
                maxString: range.maxBoundary
            };
        } else {
            let cdfCount = Math.floor(chartSize.width);
            if (bucketCount !== 0)
                cdfCount = bucketCount;

            let adjust = 0;
            if (cd.kind === "Integer") {
                if (cdfCount > range.max! - range.min!)
                    cdfCount = range.max! - range.min! + 1;
                adjust = .5;
            } else if (range.max! - range.min! <= 0) {
                cdfCount = 1;
            }

            let samplingRate = 1.0;
            if (!exact)
                samplingRate = DataRangesReceiver.samplingRate(
                    cdfCount, range.presentCount, range.missingCount, chartSize);
            // noinspection UnnecessaryLocalVariableJS
            const args: HistogramParameters = {
                cd: cd,
                min: range.min! - adjust,
                max: range.max! + adjust,
                samplingRate: samplingRate,
                bucketCount: cdfCount,
            };
            return args;
        }
    }

    private trellisLayout(windows: number, missingData: boolean): TrellisShape {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const tlc = new TrellisLayoutComputation(
            chartSize.width, Resolution.minTrellisWindowSize,
            chartSize.height, Resolution.minTrellisWindowSize,
            Resolution.lineHeight, 1.2);
        return tlc.getShape(windows, missingData);
    }

    protected commonArgs(): ReceiverCommonArgs {
        return {
            title: this.title!,
            originalPage: this.page,
            remoteObject: this.originator,
            rowCount: this.rowCount,
            schema: this.meta.schema,
            geoMetadata: this.meta.geoMetadata,
            options: this.options
        };
    }

    protected getMeta(): TableMeta {
        return this.commonArgs();
    }

    public run(ranges: (BucketsInfo | null)[]): void {
        if (ranges.length == 0)
            return;
        assert(ranges[0] != null);
        if (ranges[0].presentCount === 0) {
            this.page.reportError("No non-missing data");
            return;
        }
        this.rowCount = ranges[0].presentCount + ranges[0].missingCount;
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const exact = this.options.exact || this.isPrivate();

        // variables when drawing Trellis plots
        let trellisShape: TrellisShape | null = null;
        let windows: number | null = null;  // number of Trellis windows
        if (this.options.chartKind === "TrellisHeatmap" ||
            this.options.chartKind === "TrellisHistogram" ||
            this.options.chartKind === "Trellis2DHistogram" ||
            this.options.chartKind === "TrellisQuartiles"
        ) {
            const groupByIndex = ranges.length === 3 ? 2 : 1;
            if (this.bucketCounts[groupByIndex] !== 0) {
                windows = this.bucketCounts[groupByIndex];
            } else {
                const maxWindows =
                    Math.floor(chartSize.width / Resolution.minTrellisWindowSize) *
                    Math.floor(chartSize.height / (Resolution.minTrellisWindowSize + Resolution.lineHeight));
                if (kindIsString(this.cds[groupByIndex].kind))
                    windows = Math.min(maxWindows, ranges[groupByIndex]!.stringQuantiles!.length);
                else if (this.cds[groupByIndex].kind === "Integer")
                    windows = Math.min(maxWindows,
                        ranges[groupByIndex]!.max! - ranges[groupByIndex]!.min! + 1);
                else
                    windows = maxWindows;
            }
            trellisShape = this.trellisLayout(windows, ranges[groupByIndex]!.missingCount > 0);
            if (windows === 1) {
                // If we have a single window left do not use a Trellis display
                switch (this.options.chartKind) {
                    case "TrellisHeatmap":
                        this.options.chartKind = "Heatmap";
                        break;
                    case "TrellisHistogram":
                        this.options.chartKind = "Histogram";
                        break;
                    case "Trellis2DHistogram":
                        this.options.chartKind = "2DHistogram";
                        this.options.stacked = true;
                        break;
                    case "TrellisQuartiles":
                        this.options.chartKind = "QuartileVector";
                        break;
                    default:
                        assert(false);
                }
                if (this.title != null) {
                    this.title = new PageTitle(
                        this.title.format.replace(/\s+grouped.*/, ""),
                        this.title.provenance);
                }
            }
        }

        switch (this.options.chartKind) {
            case "CorrelationHeatmaps": {
                if (this.title == null)
                    this.title = new PageTitle(
                        "Pairwise correlations between " + ranges.length + " columns", this.provenance!);
                const colCount = ranges.length;
                if (colCount < 1) {
                    this.page.reportError("Not enough columns");
                    return;
                }
                const width = chartSize.width / (colCount - 1);
                const pixels = Math.floor(width / Resolution.minDotSize);
                // noinspection JSSuspiciousNameCombination
                const size = { width: width, height: width };
                const histoArgs = zip(this.cds, ranges,
                    (c, r) =>
                        DataRangesReceiver.computeHistogramArgs(c, r!, pixels, true, size));
                const args = createRequestArgs(histoArgs, true);
                const rr = this.originator.createCorrelationHeatmapRequest(args);
                const common = this.commonArgs();
                rr.invoke(new CorrelationHeatmapReceiver(common, args, ranges.map(r => r!), rr));
                break;
            }
            case "QuartileVector": {
                if (ranges[0].presentCount === 0) {
                    this.page.reportError("All values are missing");
                    return;
                }
                if (!kindIsNumeric(this.cds[1].kind)) {
                    this.page.reportError("Quartiles require a numeric second column " + this.cds[1].name);
                    return;
                }

                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.floor(chartSize.width / Resolution.minBarWidth);
                }
                if (this.title == null)
                    this.title = new PageTitle(
                        "Quartiles of " + this.cds[1].name +
                        " bucketed by " + this.cds[0].name, this.provenance!);

                const histoArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    exact, chartSize);
                const args: QuantilesVectorInfo = {
                    quantileCount: 4,  // we display quartiles
                    quantilesColumn: this.cds[1].name,
                    seed: Seed.instance.get(),
                    ...histoArg
                };
                //const common = this.commonArgs();
                const rr = this.originator.createQuantilesVectorRequest(args);
                rr.invoke(new QuartilesVectorReceiver(this.title, this.page, this.originator,
                    this.getMeta(), histoArg, ranges[0], this.cds[1], rr, this.options));
                break;
            }
            case "Histogram": {
                if (ranges[0].presentCount === 0) {
                    this.page.reportError("All values are missing");
                    return;
                }

                const histos: HistogramParameters[] = [];
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.floor(chartSize.width / Resolution.minBarWidth);
                }

                const histoArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    exact, chartSize);
                histos.push(histoArg);
                const cdfArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], 0,
                    exact, chartSize);
                histos.push(cdfArg);

                const args = createRequestArgs(histos, true);
                const rr = this.originator.createHistogramAndCDFRequest(args);
                rr.chain(this.operation);
                const axisData = new AxisData(this.cds[0], ranges[0], histoArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Histogram of " + this.cds[0].name, this.provenance!);
                const renderer = new HistogramReceiver(this.title, this.page,
                    this.originator.getRemoteObjectId()!, this.getMeta(), axisData, rr, cdfArg.samplingRate,
                    this.options.pieChart != null ? this.options.pieChart : false, this.options.reusePage); // TODO sampling rate?
                rr.invoke(renderer);
                break;
            }
            case "TrellisHistogram": {
                console.assert(ranges.length === 2);
                // noinspection JSObjectNullOrUndefined
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], this.bucketCounts[0],
                    exact, trellisShape!.size);
                let groups = this.bucketCounts[1] === 0 ?
                    groupByBuckets(trellisShape!) : this.bucketCounts[1];
                const wArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1]!, groups, exact, trellisShape!.size);
                // Window argument comes first
                const args = createRequestArgs([wArg, xArg], true);
                const rr = this.originator.createHistogram2DRequest(args);
                const xAxisData = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[1], ranges[1]!, wArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Histograms of " + this.cds[0].name +
                        " grouped by " + this.cds[1].name, this.provenance!);
                const renderer = new TrellisHistogramReceiver(this.title, this.page,
                    this.originator, this.getMeta(),
                    [xAxisData, groupByAxis], this.bucketCounts[0],
                    xArg.samplingRate, trellisShape!, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "TrellisQuartiles": {
                if (ranges[0].presentCount === 0) {
                    this.page.reportError("All values are missing");
                    return;
                }
                if (!kindIsNumeric(this.cds[1].kind)) {
                    this.page.reportError("Quartiles require a numeric second column " + this.cds[1].name);
                    return;
                }
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    // noinspection JSObjectNullOrUndefined
                    maxXBucketCount = Math.floor(trellisShape!.size.width / Resolution.minBarWidth);
                }
                let maxGBucketCount = this.bucketCounts[2];
                if (maxGBucketCount === 0)
                    // noinspection JSObjectNullOrUndefined
                    maxGBucketCount = trellisShape!.windowCount;

                if (this.title == null)
                    this.title = new PageTitle(
                        "Trellis quartiles of " + this.cds[1].name +
                        " bucketed by " + this.cds[0].name +
                        " grouped by " + this.cds[2].name, this.provenance!);

                const histoArg0 = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    true, chartSize);
                const histoArg2 = DataRangesReceiver.computeHistogramArgs(
                    this.cds[2], ranges[2]!, maxGBucketCount,
                    true, chartSize);
                const args: QuantilesMatrixInfo = {
                    quantileCount: 4,  // we display quartiles
                    seed: 0,  // scan all data
                    quantilesColumn: this.cds[1].name,
                    groupColumn: histoArg2,
                    ...histoArg0
                };
                const rr = this.originator.createQuantilesMatrixRequest(args);
                rr.invoke(new TrellisHistogramQuartilesReceiver(this.title, this.page, this.originator,
                    this.getMeta(), [histoArg0, histoArg2], [ranges[0], ranges[2]!],
                    this.cds[1], trellisShape!, rr,
                    this.options));
                break;
            }
            case "Trellis2DHistogram": // fall through
            case "TrellisHeatmap": {
                let maxXBucketCount;
                let maxYBucketCount;
                if (this.options.chartKind === "Trellis2DHistogram") {
                    // noinspection JSObjectNullOrUndefined
                    maxXBucketCount = Math.floor(trellisShape!.size.width / Resolution.minBarWidth);
                    maxYBucketCount = Resolution.max2DBucketCount;
                } else {
                    // noinspection JSObjectNullOrUndefined
                    maxXBucketCount = Math.floor(trellisShape!.size.width / Resolution.minDotSize);
                    maxYBucketCount = Math.floor(trellisShape!.size.height / Resolution.minDotSize);
                }
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, exact, trellisShape!.size);
                const yArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1]!, maxYBucketCount, exact, trellisShape!.size);
                const wArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[2], ranges[2]!, groupByBuckets(trellisShape!), exact, chartSize);
                // Window argument comes first
                const args = createRequestArgs([wArg, xArg, yArg], true);
                const rr = this.originator.createHistogram3DRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1]!, yArg.bucketCount);
                const groupByAxis = new AxisData(this.cds[2], ranges[2]!, wArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        this.options.chartKind.toString().replace("Trellis", "") +
                                 " (" + this.cds[0].name +
                                 ", " + this.cds[1].name +
                                 ") grouped by " + this.cds[2].name, this.provenance!);
                let renderer;
                if (this.options.chartKind === "Trellis2DHistogram")
                    renderer = new TrellisHistogram2DReceiver(this.title, this.page,
                        this.originator, this.getMeta(),
                        [xAxis, yAxis, groupByAxis], 1.0, trellisShape!, rr, this.options);
                else
                    renderer = new TrellisHeatmapReceiver(this.title, this.page,
                        this.originator, this.getMeta(),
                        [xAxis, yAxis, groupByAxis], 1.0, trellisShape!, rr, this.options.reusePage);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Heatmap": {
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0)
                    maxXBucketCount = Math.floor(chartSize.width / Resolution.minDotSize);
                let maxYBucketCount = this.bucketCounts[1];
                if (maxYBucketCount === 0)
                    maxYBucketCount = Math.floor(chartSize.height / Resolution.minDotSize);
                const xArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount, exact, chartSize);
                const yArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1]!, maxYBucketCount, exact, chartSize);
                const args = createRequestArgs([xArg, yArg], false);
                const xAxis = new AxisData(this.cds[0], ranges[0], xArg.bucketCount);
                const yAxis = new AxisData(this.cds[1], ranges[1]!, yArg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "Heatmap (" + this.cds[0].name + ", " +
                        this.cds[1].name + ")", this.provenance!);
                if (this.isPrivate()) {
                    const rr = this.originator.createHistogram2DRequest(args);
                    const renderer = new HeatmapReceiver(this.title, this.page,
                        this.originator, this.getMeta(),
                        [xAxis, yAxis], 1.0, rr, this.options.reusePage);
                    rr.chain(this.operation);
                    rr.invoke(renderer);
                } else {
                    const heatmapRequest: HeatmapRequestInfo = {
                        ...args,
                        // this.cds has all the columns that we want to display.
                        schema: this.cds
                    }
                    const rr = this.originator.createHeatmapRequest(heatmapRequest);
                    const renderer = new HeatmapWithDataReceiver(this.title, this.page,
                        this.originator, this.getMeta(), new SchemaClass(this.cds),
                        [xAxis, yAxis], 1.0, rr, this.options.reusePage);
                    rr.chain(this.operation);
                    rr.invoke(renderer);
                }
                break;
            }
            case "2DHistogram": {
                const stacked = optionToBoolean(this.options.stacked);
                let maxXBucketCount = this.bucketCounts[0];
                if (maxXBucketCount === 0) {
                    maxXBucketCount = Math.floor(chartSize.width / Resolution.minBarWidth);
                }
                let maxYBucketCount = this.bucketCounts[1];
                if (maxYBucketCount === 0) {
                    if (stacked)
                        maxYBucketCount = Resolution.max2DBucketCount;
                    else
                        maxYBucketCount = Math.floor(chartSize.width / maxXBucketCount / 2);
                }

                // The first two represent the resolution for the 2D histogram
                const xarg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], maxXBucketCount,
                    // Relative views cannot sample
                    exact || optionToBoolean(this.options.relative), chartSize);
                const yarg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[1], ranges[1]!, maxYBucketCount,
                    exact || optionToBoolean(this.options.relative), chartSize);
                // This last one represents the resolution for the CDF
                const cdfArg = DataRangesReceiver.computeHistogramArgs(
                    this.cds[0], ranges[0], 0,
                    exact, chartSize);
                const args = createRequestArgs([xarg, yarg, cdfArg], true);
                const rr = this.originator.createHistogram2DAndCDFRequest(args);
                const xAxis = new AxisData(this.cds[0], ranges[0], xarg.bucketCount);
                const yData = new AxisData(this.cds[1], ranges[1]!, yarg.bucketCount);
                if (this.title == null)
                    this.title = new PageTitle(
                        "2DHistogram (" + this.cds[0].name + ", " +
                    this.cds[1].name + ")", this.provenance!);
                const renderer = new Histogram2DReceiver(this.title, this.page,
                    this.originator, this.getMeta(),[xAxis, yData], cdfArg.samplingRate, rr, this.options);
                rr.chain(this.operation);
                rr.invoke(renderer);
                break;
            }
            case "Table":
            case "SVD Spectrum":
            case "HeavyHitters":
            case "Schema":
            case "Load":
            case "LogFile":
            case "Map":
                throw new Error("Unexpected kind " + this.options.chartKind);
            default:
                assertNever(this.options.chartKind);
        }
    }
}

/**
 * Receives the result of a an operation that produces a new dataset and initiates
 * a new range computation, which in turns initiates a chart rendering.
 */
export class NewTargetReceiver extends BaseReceiver {
    constructor(protected title: PageTitle,
                protected cds: IColumnDescription[],
                protected meta: TableMeta,
                protected bucketCounts: number[],
                page: FullPage,
                operation: ICancellable<RemoteObjectId>,
                dataset: DatasetView,
                protected options: ChartOptions) {
        super(page, operation, "Filter", dataset);
    }

    public run(value: RemoteObjectId): void {
        super.run(value); // This sets this.remoteObject.
        // cds could be longer than buckets -- e.g., for heatmaps.
        const cols = this.cds.slice(0, this.bucketCounts.length);
        const rr = this.remoteObject.createDataQuantilesRequest(cols, this.page, this.options.chartKind);
        rr.invoke(new DataRangesReceiver(this.remoteObject, this.page, rr, this.meta,
                  this.bucketCounts, this.cds, this.title, null, this.options));
    }
}
