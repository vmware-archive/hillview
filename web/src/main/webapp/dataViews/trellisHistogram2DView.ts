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
    Heatmap,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {BaseRenderer, TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {ICancellable, PartialResult} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {
    IViewSerialization,
    TrellisHistogram2DSerialization
} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {Resolution} from "../ui/ui";
import {SubMenu, TopMenu} from "../ui/menu";
import {Histogram2DPlot} from "../ui/Histogram2DPlot";
import {FilterReceiver, DataRangesCollector, TrellisShape} from "./dataRangesCollectors";
import {TrellisChartView} from "./trellisChartView";
import {NextKReceiver, TableView} from "./tableView";
import {BucketDialog} from "./histogramViewBase";

export class TrellisHistogram2DView extends TrellisChartView {
    protected hps: Histogram2DPlot[];
    protected buckets: number;
    protected xAxisData: AxisData;
    protected legendAxisData: AxisData;
    protected relative: boolean;
    protected data: Heatmap[];

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "Trellis2DHistogram");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.hps = [];
        this.data = null;

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
                }, { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying view using a table view.",
                }, { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this data without making any approximations.",
                }, { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw the histograms. " +
                        "The number of buckets must be between 1 and " + Resolution.maxBucketCount,
                }, { text: "# groups",
                    action: () => this.changeGroups(),
                    help: "Change the number of groups."
                }, { text: "heatmap...",
                    action: () => this.heatmap(),
                    help: "Show this data as a Trellis plot of heatmaps.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.buckets = Math.round(shape.size.width / Resolution.minBarWidth);
    }

    public setAxes(xAxisData: AxisData,
                   legendAxisData: AxisData,
                   groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.legendAxisData = legendAxisData;
        this.groupByAxisData = groupByAxisData;

        this.createSurfaces( (surface) => {
            const hp = new Histogram2DPlot(surface);
            this.hps.push(hp);
        });
    }

    protected onMouseMove(): void {
        const mousePosition = this.mousePosition();
        if (mousePosition.plotIndex == null ||
            mousePosition.x < 0 || mousePosition.y < 0) {
            this.pointDescription.show(false);
            return;
        }

        // TODO
    }

    protected showTable(): void {
        const newPage = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.legendAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    protected exactHistogram(): void {
        const cds = [this.xAxisData.description,
            this.legendAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.buckets, this.legendAxisData.bucketCount, this.shape.bucketCount],
                cds, null, {
                reusePage: true, relative: this.relative,
                chartKind: "Trellis2DHistogram", exact: true
            }));
    }

    protected chooseBuckets(): void {
        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.updateView(this.data,
            [bucketDialog.getBucketCount(), this.legendAxisData.bucketCount], 0));
        bucketDialog.show();
    }

    protected doChangeGroups(groupCount: number): void {
        if (groupCount == null) {
            this.page.reportError("Illegal group count");
            return;
        }
        if (groupCount === 1) {
            const cds = [this.xAxisData.description,
                this.legendAxisData.description];
            const rr = this.createDataRangesRequest(cds, this.page, "2DHistogram");
            rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
                [0, 0],
                cds, null, {
                    reusePage: true, relative: this.relative,
                    chartKind: "2DHistogram", exact: this.samplingRate >= 1
                }));
        } else {
            const cds = [this.xAxisData.description,
                this.legendAxisData.description, this.groupByAxisData.description];
            const rr = this.createDataRangesRequest(cds, this.page, "Trellis2DHistogram");
            rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
                [0, 0, groupCount],
                cds, null, {
                    reusePage: true, relative: this.relative,
                    chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1
                }));
        }
    }

    protected heatmap(): void {
        const cds = [this.xAxisData.description,
            this.legendAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.buckets, this.legendAxisData.bucketCount, this.shape.bucketCount],
            cds, null, {
                reusePage: false, relative: this.relative,
                chartKind: "TrellisHeatmap", exact: true
            }));
    }

    protected export(): void {
        // TODO
    }

    public resize(): void {
        this.updateView(this.data,
            [this.xAxisData.bucketCount, this.legendAxisData.bucketCount], 0);
    }

    public refresh(): void {
        // TODO
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHistogram2DSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.legendAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.legendAxisData.bucketCount,
            relative: this.relative,
            groupByColumn: this.groupByAxisData.description,
            xWindows: this.shape.xNum,
            yWindows: this.shape.yNum,
            groupByBucketCount: this.groupByAxisData.bucketCount
        };
        return ser;
    }

    public static reconstruct(ser: TrellisHistogram2DSerialization, page: FullPage): IDataView {
        if (ser.remoteObjectId == null || ser.rowCount == null || ser.xWindows == null ||
            ser.yWindows == null || ser.groupByBucketCount ||
            ser.samplingRate == null || ser.schema == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const shape = TrellisChartView.deserializeShape(ser, page);
        const view = new TrellisHistogram2DView(ser.remoteObjectId, ser.rowCount,
            schema, shape, ser.samplingRate, page);
        view.setAxes(new AxisData(ser.columnDescription0, null),
            new AxisData(ser.columnDescription1, null),
            new AxisData(ser.groupByColumn, null));
        return view;
    }

    public updateView(data: Heatmap[], bucketCount: number[], elapsedMs: number): void {
        this.data = data;
        for (let i = 0; i < data.length; i++) {
            const histo = data[i];
            const plot = this.hps[i];
            plot.setData(histo, this.xAxisData, this.samplingRate, false, this.schema);
            plot.draw();
        }

        // We draw the axes after drawing the data
        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom);
        const yAxis = this.hps[0].getYAxis();
        this.drawAxes(this.xAxisData.axis.axis, yAxis.axis);
        this.setupMouse();
        this.page.reportTime(elapsedMs);
    }

    protected getCombineRenderer(title: string):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(
                title,
                [this.xAxisData.description, this.legendAxisData.description,
                    this.groupByAxisData.description],
                this.schema, [0, 0, 0], page, operation, this.dataset, {
                    chartKind: "Trellis2DHistogram", relative: this.relative,
                    reusePage: false, exact: this.samplingRate >= 1
                });
        };
    }

    protected selectionCompleted(): void {
        const filter = this.getGroupBySelectionFilter();
        if (filter == null)
            return;
        const rr = this.createFilterRequest(filter);
        const title = "Filtered on " + this.schema.displayName(this.groupByAxisData.description.name);
        const renderer = new FilterReceiver(title,
            [this.xAxisData.description, this.legendAxisData.description,
                this.groupByAxisData.description],
            this.schema, [0, 0, 0], this.page, rr, this.dataset, {
                chartKind: "Trellis2DHistogram", relative: this.relative,
                reusePage: false, exact: this.samplingRate >= 1
            });
        rr.invoke(renderer);
    }
}

/**
 * Renders a Trellis plot of 2D histograms
 */
export class TrellisHistogram2DRenderer extends Receiver<Heatmap[]> {
    protected trellisView: TrellisHistogram2DView;

    constructor(title: string,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap[]>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.trellisView = new TrellisHistogram2DView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1], axes[2]);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Heatmap[]>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, [this.axes[0].bucketCount, this.axes[1].bucketCount],
            this.elapsedMilliseconds());
    }
}
