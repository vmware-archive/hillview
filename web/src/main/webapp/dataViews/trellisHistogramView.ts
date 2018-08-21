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

import {Receiver} from "../rpc";
import {
    CombineOperators,
    DataRange,
    HeatMap, HistogramBase,
    IColumnDescription,
    RemoteObjectId
} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {BigTableView, TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {ICancellable, PartialResult} from "../util";
import {AxisData} from "./axisData";
import {HistogramSerialization, IViewSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {Resolution, Size} from "../ui/ui";
import {HistogramPlot} from "../ui/histogramPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {HistogramView} from "./histogramView";
import {SubMenu, TopMenu} from "../ui/menu";

class TrellisHistogramView extends BigTableView {
    protected hps: HistogramPlot[];
    protected buckets: number;
    protected menu: TopMenu;

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Trellis");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.tabIndex = 1;
        this.hps = [];

        const table: HTMLTableElement = document.createElement("table");
        const tBody = table.createTBody();

        this.menu = new TopMenu( [{
            text: "Export",
            help: "Save the information in this view in a local file.",
            subMenu: new SubMenu([{
                text: "As CSV",
                help: "Saves the data in this view in a CSV file.",
                action: () => { this.export(); },
            }]),
        }, { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                },
                { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying view using a table view.",
                },
                { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this data without making any approximations.",
                },
                { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw the histograms. " +
                        "The number of buckets must be between 1 and " + Resolution.maxBucketCount,
                },
                { text: "correlate...",
                    action: () => this.chooseSecondColumn(),
                    help: "Draw a Trellis plot of 2-dimensional histogram using this data and another column.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);

        this.topLevel.appendChild(table);
        for (let y = 0; y < this.shape.yNum; y++) {
            const row = tBody.insertRow();
            for (let x = 0; x < this.shape.xNum; x++) {
                const cell = row.insertCell();
                const surface = new PlottingSurface(cell, this.page);
                surface.setSize(this.shape.size);
                surface.setMargins(0, 0, 0, 0);
                const hp = new HistogramPlot(surface);
                this.hps.push(hp);
            }
        }
        this.buckets = Math.round(shape.size.width / Resolution.minBarWidth);
    }

    protected showTable(): void {
        // TODO
    }

    protected exactHistogram(): void {
        // TODO
    }

    protected chooseBuckets(): void {
        // TODO
    }

    protected chooseSecondColumn(): void {
        // TODO
    }

    protected export(): void {
        // TODO
    }

    public refresh(): void {
        // TODO
    }

    public serialize(): IViewSerialization {
        // TODO
        return null;
    }

    public static reconstruct(ser: HistogramSerialization, page: FullPage): IDataView {
        // TODO
        return null;
    }

    public updateView(data: HeatMap, xAxisData: AxisData, legendAxisData: AxisData,
                      samplingRate: number,
                      elapsedMs: number): void {
        const coarsened: HistogramBase[] = [];
        let max = 0;

        for (const bucketData of data.buckets) {
            const histo: HistogramBase = {
                buckets: bucketData,
                outOfRange: 0,
                missingData: data.missingData
            };
            const coarse = HistogramView.coarsen(histo, this.buckets);
            max = Math.max(max, Math.max(...coarse.buckets));
            coarsened.push(coarse);
        }

        for (let i = 0; i < coarsened.length; i++) {
            const plot = this.hps[i];
            const coarse = coarsened[i];
            plot.setHistogram(coarse, samplingRate, xAxisData, max);
            plot.draw();
        }

        this.page.reportTime(elapsedMs);
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        // TODO
    }
}

/**
 * Renders a Trellis plot of 1D histograms
 */
export class TrellisHistogramRenderer extends Receiver<HeatMap> {
    protected trellisView: TrellisHistogramView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected cds: IColumnDescription[],
                protected ranges: DataRange[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<HeatMap>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Histograms " + schema.displayName(cds[0].name) + " grouped by " +
                schema.displayName(cds[1].name), page),
            operation, "histogram");
        this.trellisView = new TrellisHistogramView(
            remoteTable.remoteObjectId, rowCount, schema, this.shape, this.page);
        this.page.setDataView(this.trellisView);
        if (cds.length !== 2) {
            throw new Error("Expected 2 columns, got " + cds.length);
        }
    }

    public onNext(value: PartialResult<HeatMap>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        const points = value.data.buckets;
        let xPoints = 1;
        let yPoints = 1;
        if (points != null) {
            xPoints = points.length;
            yPoints = points[0] != null ? points[0].length : 1;
        }

        const xAxisData = new AxisData(this.cds[0], this.ranges[0], xPoints);
        const yAxisData = new AxisData(this.cds[1], this.ranges[1], yPoints);
        this.trellisView.updateView(value.data, xAxisData, yAxisData,
            this.samplingRate, this.elapsedMilliseconds());
    }
}

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
            }
        }
        return opt;
    }
}
