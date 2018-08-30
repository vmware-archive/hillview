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
    CombineOperators,
    Heatmap,
    Heatmap3D,
    IColumnDescription,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {TableTargetAPI} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
import {AxisData, AxisKind} from "./axisData";
import {DataRangesCollector, TrellisShape} from "./dataRangesCollectors";
import {Receiver} from "../rpc";
import {ICancellable, PartialResult} from "../util";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {IViewSerialization, TrellisHeatmapSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {mouse as d3mouse} from "d3-selection";
import {TextOverlay} from "../ui/textOverlay";
import {TrellisChartView} from "./trellisChartView";
import {NextKReceiver, TableView} from "./tableView";
import {HistogramViewBase} from "./histogramViewBase";

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatmapView extends TrellisChartView {
    private readonly colorLegend: HeatmapLegendPlot;
    private readonly legendSurface: PlottingSurface;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected hps: HeatmapPlot[];
    protected heatmaps: Heatmap3D;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected shape: TrellisShape,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "TrellisHeatmap");

        this.xAxisData = null;
        this.yAxisData = null;
        this.hps = [];
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");

        this.menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => this.refresh(),
                    help: "Redraw this view.",
                }, {
                text: "swap axes",
                    action: () => this.swapAxes(),
                    help: "Swap the X and Y axes of all plots.",
                }, { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying this view in a tabular view.",
                }, { text: "histogram",
                        action: () => this.histogram(),
                        help: "Show this data as a two-dimensional histogram.",
                    },
                ]) },
        ]);
        this.page.setMenu(this.menu);
        this.legendSurface = new HtmlPlottingSurface(this.topLevel, page);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
    }

    public static reconstruct(ser: TrellisHeatmapSerialization, page: FullPage): IDataView {
        if (ser.columnDescription0 == null || ser.columnDescription1 == null ||
            ser.samplingRate == null || ser.schema == null ||
            ser.xBucketCount == null || ser.yBucketCount == null) {
            return null;
        }
        const shape = TrellisChartView.deserializeShape(ser, page);
        if (shape == null)
            return null;

        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        const hv = new TrellisHeatmapView(ser.remoteObjectId, ser.rowCount, schema, shape, ser.samplingRate, page);
        hv.setAxes(new AxisData(ser.columnDescription0, null),
            new AxisData(ser.columnDescription1, null),
            new AxisData(ser.groupByColumn, null));
        return hv;
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHeatmapSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.yAxisData.bucketCount,
            groupByColumn: this.groupByAxisData.description,
            xWindows: this.shape.xNum,
            yWindows: this.shape.yNum,
            groupByBucketCount: this.groupByAxisData.bucketCount
        };
        return ser;
    }

    public setAxes(xAxisData: AxisData,
                   yAxisData: AxisData,
                   groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.yAxisData = yAxisData;
        this.groupByAxisData = groupByAxisData;

        this.createSurfaces((surface) => {
            const hp = new HeatmapPlot(surface, this.colorLegend, false);
            hp.clear();
            this.hps.push(hp);
        });
    }

    public combine(how: CombineOperators): void {
        // not yet used. TODO: add this menu
    }

    public refresh(): void {
        this.updateView(this.heatmaps, 0);
    }

    public resize(): void {
        this.updateView(this.heatmaps, 0);
    }

    public histogram(): void {
        const cds: IColumnDescription[] = [
            this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
        const buckets = HistogramViewBase.maxTrellis3DBuckets(this.page);
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
                chartKind: "Trellis2DHistogram",
                exact: true,
                relative: true,
                reusePage: true
            }));
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = [
            this.yAxisData.description, this.xAxisData.description, this.groupByAxisData.description];
        const buckets = HistogramViewBase.maxTrellis3DBuckets(this.page);
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
                chartKind: "TrellisHeatmap",
                exact: true,
                relative: true,
                reusePage: true
            }));
    }

    public showTable(): void {
        const newPage = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.yAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    public updateView(heatmaps: Heatmap3D, timeInMs: number): void {
        this.heatmaps = heatmaps;
        this.page.reportTime(timeInMs);
        this.colorLegend.clear();
        if (heatmaps == null || heatmaps.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        let max = 0;
        for (let i = 0; i < heatmaps.buckets.length; i++) {
            const buckets = heatmaps.buckets[i];
            const heatmap: Heatmap = {
                buckets: buckets,
                histogramMissingX: null,
                histogramMissingY: null,
                missingData: heatmaps.eitherMissing,
                totalSize: heatmaps.eitherMissing + heatmaps.totalPresent
            };
            const plot = this.hps[i];
            // The order of these operations is important
            plot.setData(heatmap, this.xAxisData, this.yAxisData);
            max = Math.max(max, plot.getMaxCount());
        }

        this.colorLegend.setData(1, max);
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }

        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom);
        // This axis is only created when the surface is drawn
        this.yAxisData.setResolution(this.shape.size.height, AxisKind.Left);
        this.drawAxes(this.xAxisData.axis, this.yAxisData.axis);

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.description.name,
                this.yAxisData.description.name,
                this.groupByAxisData.description.name,
                "count"], 40);

        // Axis labels
        const canvas = this.surface.getCanvas();
        canvas.append("text")
            .text(this.yAxisData.description.name)
            .attr("dominant-baseline", "text-before-edge");
        canvas.append("text")
            .text(this.xAxisData.description.name)
            .attr("transform", `translate(${this.surface.getChartWidth() / 2},
                      ${this.surface.getChartHeight() + this.surface.topMargin +
            this.surface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");
    }

    public onMouseMove(): void {
        const mousePosition = this.mousePosition();
        if (mousePosition.plotIndex == null ||
            mousePosition.x < 0 || mousePosition.y < 0) {
            this.pointDescription.show(false);
            return;
        }

        this.pointDescription.show(true);
        const plot = this.hps[mousePosition.plotIndex];
        const xs = this.xAxisData.invert(mousePosition.x);
        const ys = this.yAxisData.invert(mousePosition.y);
        const value = plot.getCount(mousePosition.x, mousePosition.y);
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex);

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface.getCanvas().node());
        this.pointDescription.update([xs, ys, group, value.toString()], position[0], position[1]);
    }

    protected selectionCompleted(): void {
        // TODO
    }
}

/**
 * Renders a Trellis plot of heatmaps
 */
export class TrellisHeatmapRenderer extends Receiver<Heatmap3D> {
    protected trellisView: TrellisHeatmapView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap3D>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Heatmaps " + schema.displayName(axes[0].description.name) +
                ", " + schema.displayName(axes[1].description.name) +
                " grouped by " +
                schema.displayName(axes[2].description.name), page),
            operation, "histogram");

        this.trellisView = new TrellisHeatmapView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1], axes[2]);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Heatmap3D>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, this.elapsedMilliseconds());
    }
}
