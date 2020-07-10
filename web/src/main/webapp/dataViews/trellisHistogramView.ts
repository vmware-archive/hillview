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
    Groups,
    kindIsString, RangeFilterArrayDescription,
    RemoteObjectId
} from "../javaBridge";
import {FullPage, PageTitle} from "../ui/fullPage";
import {BaseReceiver, TableTargetAPI} from "../modules";
import {DisplayName, SchemaClass} from "../schemaClass";
import {
    add,
    Converters, formatNumber, histogram2DAsCsv,
    ICancellable, makeInterval,
    PartialResult,
    percent, prefixSum, Two,
} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {IViewSerialization, TrellisHistogramSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {D3SvgElement, DragEventKind, HtmlString, Resolution} from "../ui/ui";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {CDFPlot} from "../ui/cdfPlot";
import {
    NewTargetReceiver,
    DataRangesReceiver,
    TrellisShape,
    TrellisLayoutComputation
} from "./dataRangesReceiver";
import {BucketDialog} from "./histogramViewBase";
import {TextOverlay} from "../ui/textOverlay";
import {TrellisChartView} from "./trellisChartView";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {Dialog, saveAs} from "../ui/dialog";
import {PlottingSurface} from "../ui/plottingSurface";

export class TrellisHistogramView extends TrellisChartView<Two<Groups<Groups<number>>>> {
    protected hps: HistogramPlot[];
    protected cdfs: CDFPlot[];
    protected bucketCount: number;
    protected xAxisData: AxisData;
    protected cdfDot: D3SvgElement;
    private readonly defaultProvenance: string = "Trellis histograms";

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "TrellisHistogram");
        this.hps = [];
        this.cdfs = [];

        this.menu = new TopMenu( [this.exportMenu(),
            { text: "View", help: "Change the way the data is displayed.",
                subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                },
                { text: "table",
                    action: () => this.showTable(
                        [this.xAxisData.description, this.groupByAxisData.description], this.defaultProvenance),
                    help: "Show the data underlying view using a table view.",
                },
                { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this data without making any approximations.",
                },
                { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw the histograms. ",
                }, { text: "# groups",
                    action: () => this.changeGroups(),
                    help: "Change the number of groups."
                }, { text: "correlate...",
                    action: () => this.chooseSecondColumn(),
                    help: "Draw a Trellis plot of 2-dimensional histogram using this data and another column.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.createDiv("chart");
        this.createDiv("summary");
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.hps = [];
        this.cdfs = [];
        this.createAllSurfaces((surface) => {
            const hp = new HistogramPlot(surface);
            this.hps.push(hp);
            const cdfp = new CDFPlot(surface);
            this.cdfs.push(cdfp);
        });
    }

    public setAxes(xAxisData: AxisData, groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.groupByAxisData = groupByAxisData;
    }

    protected doChangeGroups(groupCount: number): void {
        if (groupCount == null) {
            this.page.reportError("Illegal group count");
            return;
        }
        if (groupCount === 1) {
            const cds = [this.xAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "Histogram");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0], cds, null, "change groups",{
                    reusePage: true, relative: false,
                    chartKind: "Histogram", exact: this.samplingRate >= 1, pieChart: false
                }));
        } else {
            const cds = [this.xAxisData.description, this.groupByAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHistogram");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0, groupCount], cds, null, "change groups", {
                    reusePage: true, relative: false,
                    chartKind: "TrellisHistogram", exact: this.samplingRate >= 1
                }));
        }
    }

    protected exactHistogram(): void {
        const cds = [this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.bucketCount, this.shape.windowCount], cds, null, "exact counts",{
                reusePage: true, relative: false,
                chartKind: "TrellisHistogram", exact: true
            }));
    }

    protected chooseBuckets(): void {
        const bucketDialog = new BucketDialog(
            this.bucketCount, Resolution.maxBuckets(this.page.getWidthInPixels()));
        bucketDialog.setAction(() => {
            const ct = bucketDialog.getBucketCount();
            if (ct != null)
                this.updateView(this.data, ct);
        });
        bucketDialog.show();
    }

    public chooseSecondColumn(): void {
        const columns: DisplayName[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name === this.xAxisData.description.name ||
                col.name === this.groupByAxisData.description.name)
                continue;
            columns.push(this.schema.displayName(col.name));
        }
        if (columns.length === 0) {
            this.page.reportError("No other acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column",
            "Select a second column to use for displaying a Trellis plot of 2D histograms.");
        dialog.addColumnSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing a Trellis plot of two-dimensional histogram.");
        dialog.setAction(() => this.showSecondColumn(dialog.getColumnName("column")));
        dialog.show();
    }

    protected showSecondColumn(colName: DisplayName): void {
        const col = this.schema.findByDisplayName(colName);
        const cds = [this.xAxisData.description, col, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.bucketCount, 0, this.shape.windowCount], cds, null, this.defaultProvenance,{
                reusePage: true, relative: false,
                chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1
            }));
    }

    protected export(): void {
        const lines: string[] = histogram2DAsCsv(
            this.data.first, this.schema, [this.xAxisData, this.groupByAxisData]);
        const fileName = "trellis-histogram.csv";
        saveAs(fileName, lines.join("\n"));
    }

    public resize(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        this.shape = TrellisLayoutComputation.resize(chartSize.width, chartSize.height, this.shape);
        this.updateView(this.data, this.bucketCount);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.groupByAxisData.description];
        const ranges = [this.xAxisData.dataRange, this.groupByAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema,
            [this.bucketCount, this.groupByAxisData.bucketCount],
            cds, this.page.title, null,{
                chartKind: "TrellisHistogram", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const ser: TrellisHistogramSerialization = {
            ...super.serialize(),
            ...this.shape,
            isPie: false,
            bucketCount: this.bucketCount,
            samplingRate: this.samplingRate,
            columnDescription: this.xAxisData.description,
            groupByColumn: this.groupByAxisData.description
        };
        return ser;
    }

    public static reconstruct(ser: TrellisHistogramSerialization, page: FullPage): IDataView {
        if (ser.remoteObjectId == null || ser.rowCount == null || ser.xWindows == null ||
            ser.yWindows == null || ser.windowCount ||
            ser.samplingRate == null || ser.schema == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const shape = TrellisChartView.deserializeShape(ser, page);
        const view = new TrellisHistogramView(ser.remoteObjectId, ser.rowCount,
            schema, shape, ser.samplingRate, page);
        view.setAxes(new AxisData(ser.columnDescription, null, ser.bucketCount),
            new AxisData(ser.groupByColumn, null, ser.windowCount));
        return view;
    }

    private static coarsen(cdf: Groups<number>, bucketCount: number): Groups<number> {
        const cdfBucketCount = cdf.perBucket.length;
        if (bucketCount === cdfBucketCount)
            return cdf;

        const buckets = [];
        const bucketWidth = cdfBucketCount / bucketCount;
        for (let i = 0; i < bucketCount; i++) {
            let sum = 0;
            const leftBoundary = i * bucketWidth - .5;
            const rightBoundary = leftBoundary + bucketWidth;
            for (let j = Math.ceil(leftBoundary); j < rightBoundary; j++) {
                console.assert(j < cdf.perBucket.length);
                sum += cdf.perBucket[j];
            }
            buckets.push(Math.max(sum, 0));
        }

        // noinspection UnnecessaryLocalVariableJS
        const hist: Groups<number> = {
            perBucket: buckets,
            perMissing: cdf.perMissing };
        return hist;
    }

    public updateView(data: Two<Groups<Groups<number>>>, bucketCount: number): void {
        if (data == null || data.first === null || data.first.perBucket == null)
            return;

        const histos = data.first;
        const confidences = data.second;

        this.createNewSurfaces();
        if (this.isPrivate()) {
            const cols = [this.xAxisData.description.name, this.groupByAxisData.description.name];
            const eps = this.dataset.getEpsilon(cols);
            this.page.setEpsilon(eps, cols);
        }
        if (bucketCount !== 0)
            this.bucketCount = bucketCount;
        else
            this.bucketCount = Math.min(Math.round(this.shape.size.width / Resolution.minBarWidth),
                histos.perBucket[0].perBucket.length);

        this.data = data;
        const coarsened: Groups<number>[] = [];
        let max = 0;
        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";

        for (let i = 0; i < histos.perBucket.length; i++) {
            const bucketData = histos.perBucket[i];
            const cdfp = this.cdfs[i];
            cdfp.setData(prefixSum(bucketData.perBucket.map((b) => Math.max(0, b))), discrete);

            const coarse = TrellisHistogramView.coarsen(bucketData, this.bucketCount);
            max = Math.max(max, Math.max(...coarse.perBucket));
            coarsened.push(coarse);
        }
        if (histos.perMissing.perBucket.reduce(add) > 0) {
            const bucketData = histos.perMissing;
            const cdfp = this.cdfs[histos.perBucket.length];
            cdfp.setData(prefixSum(bucketData.perBucket.map((b) => Math.max(0, b))), discrete);
            const coarse = TrellisHistogramView.coarsen(bucketData, this.bucketCount);
            max = Math.max(max, Math.max(...coarse.perBucket));
            coarsened.push(coarse);
        }

        for (let i = 0; i < coarsened.length; i++) {
            const plot = this.hps[i];
            const confidence = {
                perBucket: confidences != null ? confidences.perBucket[i].perBucket : null,
                perMissing: confidences != null ? confidences.perBucket[i].perMissing : null
            };
            plot.setHistogram({ first: coarsened[i], second: confidence }, this.samplingRate,
                this.xAxisData,
                max, this.page.dataset.isPrivate(), this.rowCount);
            plot.displayAxes = false;
            plot.draw();
            plot.border(1);
            this.cdfs[i].draw();
        }

        // We draw the axes after drawing the data
        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom, PlottingSurface.bottomMargin);
        // This axis is only created when the surface is drawn
        const yAxis = this.hps[0].getYAxis();
        this.drawAxes(this.xAxisData.axis, yAxis);

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.schema),
                this.groupByAxisData.getDisplayNameString(this.schema),
                "count", "cdf"], 40);
        this.cdfDot = this.surface.getChart()
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");
        const summary = new HtmlString(formatNumber(this.rowCount) + " points");
        summary.setInnerHtml(this.summaryDiv);
    }

    protected onMouseMove(): void {
        const mousePosition = this.checkMouseBounds();
        if (mousePosition == null)
            return;

        this.pointDescription.show(true);
        const plot = this.hps[mousePosition.plotIndex];
        const xs = this.xAxisData.invert(mousePosition.x);
        const value = plot.get(mousePosition.x);
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex, 40);

        const cdfPlot = this.cdfs[mousePosition.plotIndex];

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface.getCanvas().node());
        const cdfPos = cdfPlot.getY(mousePosition.x);
        this.cdfDot.attr("cx", position[0] - this.surface.leftMargin);
        this.cdfDot.attr("cy", (1 - cdfPos) * cdfPlot.getChartHeight() + this.shape.headerHeight +
            mousePosition.plotYIndex * (this.shape.size.height + this.shape.headerHeight));
        const perc = percent(cdfPos);
        this.pointDescription.update([xs, group, makeInterval(value), perc], position[0], position[1]);
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return this.groupByAxisData;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                // TODO
                return null;
        }
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

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new NewTargetReceiver(title, [this.xAxisData.description, this.groupByAxisData.description],
                this.schema, [0, 0], page, operation, this.dataset, {
                chartKind: "TrellisHistogram", relative: false,
                reusePage: false, exact: this.samplingRate >= 1
            });
        };
    }

    protected selectionCompleted(): void {
        const local = this.selectionIsLocal();
        let filter: RangeFilterArrayDescription;
        if (local != null) {
            const origin = this.canvasToChart(this.selectionOrigin);
            const left = this.position(origin.x, origin.y);
            const end = this.canvasToChart(this.selectionEnd);
            const right = this.position(end.x, end.y);
            filter = {
                filters: [this.xAxisData.getFilter(left.x, right.x)],
                complement: d3event.sourceEvent.ctrlKey,
            };
        } else {
            filter = this.getGroupBySelectionFilter();
        }
        if (filter == null)
            return;
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle(this.page.title.format, Converters.filterArrayDescription(filter));
        const renderer = new NewTargetReceiver(title, [this.xAxisData.description, this.groupByAxisData.description],
            this.schema, [0, 0], this.page, rr, this.dataset, {
            chartKind: "TrellisHistogram", relative: false,
            reusePage: false, exact: this.samplingRate >= 1
        });
        rr.invoke(renderer);
    }
}

/**
 * Renders a Trellis plot of 1D histograms
 */
export class TrellisHistogramReceiver extends Receiver<Two<Groups<Groups<number>>>> {
    protected trellisView: TrellisHistogramView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected bucketCount: number,
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Two<Groups<Groups<number>>>>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.trellisView = new TrellisHistogramView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1]);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Two<Groups<Groups<number>>>>): void {
        super.onNext(value);
        if (value == null || value.data == null) {
            return;
        }

        this.trellisView.updateView(value.data, this.bucketCount);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.trellisView.updateCompleted(this.elapsedMilliseconds());
    }
}
