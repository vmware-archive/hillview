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

import {mouse as d3mouse} from "d3-selection";
import {IViewSerialization, QuantileVectorSerialization} from "../datasetView";
import {
    BucketsInfo, Groups, HistogramInfo,
    IColumnDescription,
    RemoteObjectId, SampleSet,
} from "../javaBridge";
import {Receiver, RpcRequest} from "../rpc";
import {BaseReceiver, TableTargetAPI} from "../modules";
import {IDataView} from "../ui/dataview";
import {FullPage, PageTitle} from "../ui/fullPage";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {ChartOptions, DragEventKind, Resolution} from "../ui/ui";
import {
    assert,
    assertNever,
    Converters,
    describeQuartiles, Exporter,
    ICancellable,
    PartialResult,

} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {DataRangesReceiver, NewTargetReceiver} from "./dataRangesReceiver";
import {DisplayName} from "../schemaClass";
import {Quartiles2DPlot} from "../ui/quartiles2DPlot";
import {saveAs} from "../ui/dialog";
import {TableMeta} from "../ui/receiver";

/**
 * This class is responsible for rendering a vector of quartiles.
 * Each quartile is for a bucket.
 */
export class QuartilesHistogramView extends HistogramViewBase<Groups<SampleSet>> {
    protected plot: Quartiles2DPlot;
    protected yAxisData: AxisData;
    private readonly defaultProvenance = "From quartile histogram";

    constructor(remoteObjectId: RemoteObjectId, meta: TableMeta,
                protected qCol: IColumnDescription, page: FullPage) {
        super(remoteObjectId, meta, page, "QuartileVector");

        this.menu = new TopMenu( [this.exportMenu(), {
            text: "View",
            help: "Change the way the data is displayed.",
            subMenu: new SubMenu([{
                text: "refresh",
                action: () => { this.refresh(); },
                help: "Redraw this view",
            }, {
                text: "table",
                action: () => this.showTable([this.xAxisData.description, this.qCol], this.defaultProvenance),
                help: "Show the data underlying this plot in a tabular view. ",
            }, {
                text: "heatmap",
                action: () => { this.doHeatmap(); },
                help: "Plot this data as a heatmap view.",
            }, {
                text: "2D histogram",
                action: () => { this.do2DHistogram(); },
                help: "Plot this data as a 2D histogram.",
            }, {
                text: "1D histogram",
                action: () => { this.doHistogram(); },
                help: "Plot the X column as 1 D histogam.",
            }, {
                text: "group by...",
                action: () => {
                    this.trellis();
                },
                help: "Group data by a third column.",
            }]) },
            page.dataset!.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
    }

    public trellis(): void {
        const columns: DisplayName[] = this.getSchema().displayNamesExcluding(
            [this.xAxisData.description.name, this.qCol.name]);
        this.chooseTrellis(columns);
    }

    protected showTrellis(colName: DisplayName): void {
        const groupBy = this.getSchema().findByDisplayName(colName)!;
        const cds: IColumnDescription[] = [
            this.xAxisData.description,
            this.qCol,
            groupBy];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisQuartiles");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0, 0], cds, null, this.defaultProvenance,{
                reusePage: false, chartKind: "TrellisQuartiles",
            }));
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {});
        this.plot = new Quartiles2DPlot(this.surface);
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return null;
            case "YAxis":
                return this.yAxisData;
            case "XAxis":
                return this.xAxisData;
            default:
                assertNever(event);
        }
        return null;
    }

    public updateView(qv: Groups<SampleSet>): void {
        this.createNewSurfaces();
        this.data = qv;

        const bucketCount = this.xAxisData.bucketCount;
        this.plot.setData(qv, this.getSchema(), this.meta.rowCount, this.xAxisData, false, null);
        this.plot.draw();
        this.setupMouse();
        this.yAxisData = new AxisData(this.qCol, this.plot.yDataRange(), 0);
        this.yAxisData.setResolution(this.plot.getChartHeight(), AxisKind.Left, 0);

        assert(this.surface != null);
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.getSchema())!, "bucket",
                "max", "q3", "median", "q1", "min", "count", "missing"], 40);
        this.pointDescription.show(false);
        this.standardSummary();
        assert(this.summary != null);
        this.summary.set("buckets", bucketCount);
        this.summary.display();
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: QuantileVectorSerialization = {
            ...super.serialize(),
            xRange: this.xAxisData.dataRange,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.qCol,
            xBucketCount: this.xAxisData.bucketCount,
        };
        return result;
    }

    public static reconstruct(ser: QuantileVectorSerialization, page: FullPage): IDataView | null {
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const xPoints: number = ser.xBucketCount;
        const args = this.validateSerialization(ser);
        if (cd0 === null || cd1 === null || args === null ||
            xPoints === null || ser.xRange === null)
            return null;

        const hv = new QuartilesHistogramView(ser.remoteObjectId, args, cd1, page);
        hv.setAxis(new AxisData(cd0, ser.xRange, ser.xBucketCount));
        return hv;
    }

    public setAxis(xAxisData: AxisData): void {
        this.xAxisData = xAxisData;
    }

    public doHeatmap(): void {
        const cds = [this.xAxisData.description, this.qCol];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0], cds, null, this.defaultProvenance,{
            reusePage: false,
            chartKind: "Heatmap",
            exact: true
        }));
    }

    public doHistogram(): void {
        const cds = [this.xAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Histogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [this.xAxisData.bucketCount], cds, null, this.defaultProvenance, {
                reusePage: false,
                chartKind: "Histogram"
            }));
    }

    public do2DHistogram(): void {
        const cds = [this.xAxisData.description, this.qCol];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [this.xAxisData.bucketCount, 0], cds, null, this.defaultProvenance, {
                reusePage: false,
                chartKind: "2DHistogram"
            }));
    }

    public export(): void {
        const lines: string[] = Exporter.quartileAsCsv(this.data, this.getSchema(), this.xAxisData);
        const fileName = "quantiles2d.csv";
        saveAs(fileName, lines.join("\n"));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new NewTargetReceiver(title, [this.xAxisData.description, this.qCol],
                this.meta, [0, 0], page, operation, this.dataset, {
                exact: true, chartKind: "QuartileVector",
                reusePage: false
            });
        };
    }

    public chooseBuckets(): void {
        if (this == null)
            return;
        const bucketDialog = new BucketDialog(
            this.xAxisData.bucketCount, Resolution.maxBuckets(this.page.getWidthInPixels()));
        bucketDialog.setAction(() => {
            const bucketCount = bucketDialog.getBucketCount();
            if (bucketCount == null)
                return;
            const cds = [this.xAxisData.description, this.qCol];
            const rr = this.createDataQuantilesRequest(cds, this.page, "QuartileVector");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
                [bucketCount], cds, null, "changed buckets",{
                    reusePage: true, chartKind: "QuartileVector"
                }));
        });
        bucketDialog.show();
    }

    public resize(): void {
        if (this == null)
            return;
        this.updateView(this.data);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.qCol];
        const ranges = [this.xAxisData.dataRange];
        const receiver = new DataRangesReceiver(this,
            this.page, null, this.meta,
            [this.xAxisData.bucketCount],
            cds, this.page.title, null,{
                chartKind: "QuartileVector", exact: true,
                reusePage: true
            });
        receiver.run(ranges);
        receiver.finished();
    }

    public onMouseEnter(): void {
        super.onMouseEnter();
    }

    public onMouseLeave(): void {
        super.onMouseLeave();
    }

    /**
     * Handles mouse movements in the canvas area only.
     */
    public onMouseMove(): void {
        const position = d3mouse(this.surface!.getChart().node());
        // note: this position is within the chart
        const mouseX = position[0];
        const mouseY = position[1];

        let xs = "";
        // Use the plot scale, not the yData to invert.  That's the
        // one which is used to draw the axis.
        let bucketDesc = "";
        let min = "", q1 = "", q2 = "", q3 = "", max = "", missing = "", count = "";
        if (this.xAxisData.scale != null) {
            xs = this.xAxisData.invert(position[0]);
            if (this.data != null) {
                const bucket = this.plot.getBucketIndex(position[0]);
                if (bucket < 0)
                    return;
                bucketDesc = this.xAxisData.bucketDescription(bucket, 20);
                const qv = this.data.perBucket[bucket];
                [count, missing, min, q1, q2, q3, max] = describeQuartiles(qv);
            }
        }
        this.pointDescription!.update([xs, bucketDesc, max, q3, q2, q1, min, count, missing], mouseX, mouseY);
    }

    protected dragMove(): boolean {
        return this.dragMoveRectangle();
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd() || this.selectionOrigin == null)
            return false;
        const position = d3mouse(this.surface!.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
        return true;
    }

    /**
     * * xl and xr are coordinates of the mouse position within the
     * canvas or legend rectangle respectively.
     */
    protected selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        const f = this.filterSelectionRectangle(xl, xr, yl, yr, this.xAxisData, this.yAxisData);
        if (f == null)
            return;
        const rr = this.createFilterRequest(f);
        const renderer = new NewTargetReceiver(
            new PageTitle(this.page.title.format,
                Converters.filterArrayDescription(f)),
            [this.xAxisData.description, this.qCol], this.meta,
            [0], this.page, rr, this.dataset, {
            chartKind: "QuartileVector", reusePage: false,
        });
        rr.invoke(renderer);
    }
}

export class QuartilesVectorReceiver extends Receiver<Groups<SampleSet>> {
    protected view: QuartilesHistogramView;

    constructor(title: PageTitle,
                page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected meta: TableMeta,
                protected histoArgs: HistogramInfo,
                protected range: BucketsInfo,
                protected quantilesCol: IColumnDescription,
                operation: RpcRequest<Groups<SampleSet>>,
                protected options: ChartOptions) {
        super(options.reusePage ? page : page.dataset!.newPage(title, page), operation, "quartiles");
        this.view = new QuartilesHistogramView(
            this.remoteObject.remoteObjectId, meta, quantilesCol, this.page);
        const axisData = new AxisData(histoArgs.cd, range, histoArgs.bucketCount);
        this.view.setAxis(axisData);
    }

    public onNext(value: PartialResult<Groups<SampleSet>>): void {
        super.onNext(value);
        if (value == null)
            return;
        this.view.updateView(value.data);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}
