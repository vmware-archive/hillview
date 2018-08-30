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
    Heatmap,
    HistogramBase,
    kindIsString,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {ICancellable, PartialResult, percent, significantDigits} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {IViewSerialization, TrellisHistogramSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {D3SvgElement, Resolution} from "../ui/ui";
import {HistogramPlot} from "../ui/histogramPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {HistogramView} from "./histogramView";
import {SubMenu, TopMenu} from "../ui/menu";
import {CDFPlot} from "../ui/CDFPlot";
import {DataRangesCollector, TrellisShape} from "./dataRangesCollectors";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {TextOverlay} from "../ui/textOverlay";
import {TrellisChartView} from "./trellisChartView";
import {mouse as d3mouse} from "d3-selection";
import {NextKReceiver, TableView} from "./tableView";
import {Dialog} from "../ui/dialog";

export class TrellisHistogramView extends TrellisChartView {
    protected hps: HistogramPlot[];
    protected cdfs: CDFPlot[];
    protected bucketCount: number;
    protected xAxisData: AxisData;
    protected data: Heatmap;
    protected cdfDot: D3SvgElement;

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "TrellisHistogram");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.hps = [];
        this.cdfs = [];

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
    }

    public setAxes(xAxisData: AxisData, groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.groupByAxisData = groupByAxisData;

        this.createSurfaces((surface) => {
            const hp = new HistogramPlot(surface);
            this.hps.push(hp);
            const cdfp = new CDFPlot(surface);
            this.cdfs.push(cdfp);
        });
    }

    protected showTable(): void {
        const newPage = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true,
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    protected exactHistogram(): void {
        const buckets = HistogramViewBase.maxTrellis2DBuckets(this.page);
        const cds = [this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.bucketCount, this.shape.bucketCount], cds, null, {
                reusePage: true, relative: false,
                chartKind: "TrellisHistogram", exact: true
            }));
    }

    protected chooseBuckets(): void {
        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.updateView(this.data, bucketDialog.getBucketCount(), 0));
        bucketDialog.show();
    }
    public chooseSecondColumn(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name === this.xAxisData.description.name ||
                col.name === this.groupByAxisData.description.name)
                continue;
            columns.push(col.name);
        }
        if (columns.length === 0) {
            this.page.reportError("No other acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column",
            "Select a second column to use for displaying a Trellis plot of 2D histograms.");
        dialog.addSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing a Trellis plot of two-dimensional histogram.");
        dialog.setAction(() => this.showSecondColumn(dialog.getFieldValue("column")));
        dialog.show();
    }

    protected showSecondColumn(colName: string): void {
        const col = this.schema.find(colName);
        const buckets = HistogramViewBase.maxTrellis3DBuckets(this.page);
        const cds = [this.xAxisData.description, col, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.bucketCount, 0, this.shape.bucketCount], cds, null, {
                reusePage: true, relative: false,
                chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1
            }));
    }

    protected export(): void {
        // TODO
    }

    public resize(): void {
        this.updateView(this.data, this.bucketCount, 0);
    }

    public refresh(): void {
        const buckets = HistogramViewBase.maxTrellis2DBuckets(this.page);
        const cds = [this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.bucketCount, this.shape.bucketCount], cds, null, {
                reusePage: true, relative: false,
                chartKind: "TrellisHistogram", exact: this.samplingRate >= 1
            }));
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHistogramSerialization = {
            ...super.serialize(),
            bucketCount: this.bucketCount,
            samplingRate: this.samplingRate,
            columnDescription: this.xAxisData.description,
            groupByColumn: this.groupByAxisData.description,
            xWindows: this.shape.xNum,
            yWindows: this.shape.yNum,
            groupByBucketCount: this.shape.bucketCount
        };
        return ser;
    }

    public static reconstruct(ser: TrellisHistogramSerialization, page: FullPage): IDataView {
        if (ser.remoteObjectId == null || ser.rowCount == null || ser.xWindows == null ||
            ser.yWindows == null || ser.groupByBucketCount ||
            ser.samplingRate == null || ser.schema == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const shape = TrellisChartView.deserializeShape(ser, page);
        const view = new TrellisHistogramView(ser.remoteObjectId, ser.rowCount,
            schema, shape, ser.samplingRate, page);
        view.setAxes(new AxisData(ser.columnDescription, null),
            new AxisData(ser.groupByColumn, null));
        return view;
    }

    public updateView(data: Heatmap,
                      bucketCount: number,
                      elapsedMs: number): void {
        if (bucketCount !== 0)
            this.bucketCount = bucketCount;
        else
            this.bucketCount = Math.round(this.shape.size.width / Resolution.minBarWidth);
        this.data = data;
        const coarsened: HistogramBase[] = [];
        let max = 0;
        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";

        for (let i = 0; i < data.buckets.length; i++) {
            const bucketData = data.buckets[i];
            const histo: HistogramBase = {
                buckets: bucketData,
                missingData: data.missingData
            };

            const cdfp = this.cdfs[i];
            cdfp.setData(histo, discrete);

            const coarse = HistogramView.coarsen(histo, this.bucketCount);
            max = Math.max(max, Math.max(...coarse.buckets));
            coarsened.push(coarse);
        }

        for (let i = 0; i < coarsened.length; i++) {
            const plot = this.hps[i];
            const coarse = coarsened[i];
            plot.setHistogram(coarse, this.samplingRate, this.xAxisData, max);
            plot.draw();
            plot.border(1);
            this.cdfs[i].draw();
        }

        // We draw the axes after drawing the data
        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom);
        // This axis is only created when the surface is drawn
        const yAxis = this.hps[0].getYAxis();
        this.drawAxes(this.xAxisData.axis, yAxis);

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.description.name,
                this.groupByAxisData.description.name,
                "count", "cdf"], 40);
        this.page.reportTime(elapsedMs);
        this.cdfDot = this.surface.getChart()
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");
    }

    protected onMouseMove(): void {
        const mousePosition = this.mousePosition();
        if (mousePosition.plotIndex == null ||
            mousePosition.x < 0 || mousePosition.y < 0) {
            this.pointDescription.show(false);
            return;
        }

        this.pointDescription.show(true);
        const plot = this.hps[mousePosition.plotIndex];
        const xs = this.xAxisData.invert(mousePosition.x);
        const value = plot.get(mousePosition.x);
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex);

        const cdfPlot = this.cdfs[mousePosition.plotIndex];

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface.getCanvas().node());
        const cdfPos = cdfPlot.getY(mousePosition.x);
        this.cdfDot.attr("cx", position[0] - this.surface.leftMargin);
        this.cdfDot.attr("cy", (1 - cdfPos) * cdfPlot.getChartHeight() + this.shape.headerHeight +
            mousePosition.plotYIndex * (this.shape.size.height + this.shape.headerHeight));
        const perc = percent(cdfPos);
        this.pointDescription.update([xs, group, significantDigits(value), perc], position[0], position[1]);
    }

    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;
        const index = this.selectionIsLocal();
        if (index != null) {
            // Adjust the selection rectangle size to cover the whole vertical space
            this.selectionRectangle
                .attr("height", this.shape.size.height)
                .attr("y", this.coordinates[index].y);
        }
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        // TODO
    }
}

/**
 * Renders a Trellis plot of 1D histograms
 */
export class TrellisHistogramRenderer extends Receiver<Heatmap> {
    protected trellisView: TrellisHistogramView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Histograms " + schema.displayName(axes[0].description.name) + " grouped by " +
                schema.displayName(axes[1].description.name), page),
            operation, "histogram");
        this.trellisView = new TrellisHistogramView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1]);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Heatmap>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, this.axes[0].bucketCount, this.elapsedMilliseconds());
    }
}
