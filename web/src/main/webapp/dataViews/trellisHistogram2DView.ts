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
    Heatmap,
    IColumnDescription,
    RemoteObjectId
} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {ICancellable, PartialResult} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {
    IViewSerialization,
    TrellisHistogram2DSerialization, TrellisHistogramSerialization
} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {Resolution} from "../ui/ui";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {SubMenu, TopMenu} from "../ui/menu";
import {Histogram2DPlot} from "../ui/Histogram2DPlot";
import {TrellisShape} from "./dataRangesCollectors";
import {ChartView} from "./chartView";

export class TrellisHistogram2DView extends ChartView {
    protected hps: Histogram2DPlot[];
    protected buckets: number;
    protected xAxisData: AxisData;
    protected legendAxisData: AxisData;

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Trellis2DHistogram");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.tabIndex = 1;
        this.hps = [];

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
                { text: "heatmap...",
                    action: () => this.heatmap(),
                    help: "Show this data as a Trellis plot of heatmaps.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.surface = new HtmlPlottingSurface(this.topLevel, this.page);
        this.surface.create();

        for (let y = 0; y < this.shape.yNum; y++) {
            for (let x = 0; x < this.shape.xNum; x++) {
                const surface = this.surface.createChildSurface(
                    PlottingSurface.leftMargin + x * this.shape.size.width,
                    y * this.shape.size.height + PlottingSurface.topMargin);
                surface.setSize(this.shape.size);
                surface.setMargins(0, 0, 0, 0);
                const hp = new Histogram2DPlot(surface);
                this.hps.push(hp);
            }
        }
        this.buckets = Math.round(shape.size.width / Resolution.minBarWidth);
    }

    public setAxes(xAxisData: AxisData,
                   legendAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.legendAxisData = legendAxisData;
    }

    protected onMouseMove(): void {
        // TODO
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

    protected heatmap(): void {
        // TODO
    }

    protected export(): void {
        // TODO
    }

    public refresh(): void {
        // TODO
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHistogramSerialization = {
            ...super.serialize()
            // TODO
        };
        return ser;
    }

    public static reconstruct(ser: TrellisHistogram2DSerialization, page: FullPage): IDataView {
        // TODO
        return null;
    }

    public updateView(data: Heatmap[], elapsedMs: number): void {
        for (let i = 0; i < data.length; i++) {
            const histo = data[i];
            const plot = this.hps[i];
            plot.setData(histo, this.xAxisData, this.samplingRate, false);
            plot.draw();
        }

        // We draw the axes after drawing the data
        const borderWidth = 0;
        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom);
        for (let i = 0; i < this.shape.xNum; i++) {
            this.surface
                .getCanvas()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(
                    ${PlottingSurface.leftMargin + i * (this.shape.size.width + borderWidth)}, 
                    ${PlottingSurface.topMargin + (this.shape.size.height + borderWidth) * this.shape.yNum})`)
                .call(this.xAxisData.axis);
        }

        const yAxis = this.hps[0].getYAxis();
        for (let i = 0; i < this.shape.yNum; i++) {
            this.surface.getCanvas()
                .append("g")
                .attr("class", "y-axis")
                .attr("transform", `translate(${PlottingSurface.leftMargin},
                                              ${PlottingSurface.topMargin + i * 
                                               (this.shape.size.height + borderWidth)})`)
                .call(yAxis);
        }

        this.setupMouse();
        this.page.reportTime(elapsedMs);
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        // TODO
    }
}

/**
 * Renders a Trellis plot of 2D histograms
 */
export class TrellisHistogram2DRenderer extends Receiver<Heatmap[]> {
    protected trellisView: TrellisHistogram2DView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected cds: IColumnDescription[],
                protected ranges: DataRange[],
                protected bucketCounts: number[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap[]>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Histograms " + schema.displayName(cds[0].name) + " grouped by " +
                schema.displayName(cds[1].name), page),
            operation, "histogram");

        console.assert(cds.length === 2 && ranges.length === 2 && bucketCounts.length === 2);
        const xAxisData = new AxisData(cds[0], ranges[0]);
        xAxisData.setBucketCount(bucketCounts[0]);
        const yAxisData = new AxisData(cds[1], ranges[1]);
        yAxisData.setBucketCount(bucketCounts[1]);
        this.trellisView = new TrellisHistogram2DView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(xAxisData, yAxisData);
        this.page.setDataView(this.trellisView);
        if (cds.length !== 2) {
            throw new Error("Expected 2 columns, got " + cds.length);
        }
    }

    public onNext(value: PartialResult<Heatmap[]>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, this.elapsedMilliseconds());
    }
}
