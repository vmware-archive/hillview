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
import {ColumnSortOrientation, IColumnDescription, RecordOrder, RemoteObjectId} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {ICDFPlot} from "../ui/cdfPlot";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {D3SvgElement, Resolution, ViewKind} from "../ui/ui";
import {ChartView} from "./chartView";
import {AxisData} from "./axisData";
import {NextKReceiver, TableView} from "./tableView";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase<D> extends ChartView<D> {
    protected summary: HTMLElement;
    protected cdfDot: D3SvgElement;
    protected cdfPlot: ICDFPlot;
    protected chartDiv: HTMLDivElement;
    // protected scrollBar: ScrollBar;
    public xAxisData: AxisData;

    protected constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage, viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.chartDiv = this.createChartDiv();
        this.cdfDot = null;
        // this.scrollBar = new ScrollBar(this, true);
        // this.topLevel.appendChild(this.scrollBar.getHTMLRepresentation());

        const summaryContainer = document.createElement("div");
        this.topLevel.appendChild(summaryContainer);
        this.summary = document.createElement("div");
        summaryContainer.appendChild(this.summary);
    }

    public abstract resize(): void;

    /**
     * Dragging started in the canvas.
     */
    protected dragStart(): void {
        super.dragStart();
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    /**
     * The mouse moved in the canvas.
     */
    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.surface.getChartHeight();

        if (width < 0) {
            ox = x;
            width = -width;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", this.surface.topMargin)
            .attr("width", width)
            .attr("height", height);
        return true;
    }

    // show the table corresponding to the data in the histogram
    protected showTable(axes: IColumnDescription[], provenance: string): void {
        const orientation: ColumnSortOrientation[] = [];
        for (const a of axes) {
            orientation.push({
                columnDescription: a,
                isAscending: true
            });
        }
        const order = new RecordOrder(orientation);

        const page = this.dataset.newPage(new PageTitle("Table", provenance), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        page.setDataView(table);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }
}

/**
 * A dialog that queries the user about the number of buckets to use.
 */
export class BucketDialog extends Dialog {
    constructor(count: number, max: number) {
        super("Set buckets", "Change the number of buckets (bars) used to display the histogram.");
        const input = this.addTextField("n_buckets", "Number of buckets:", FieldKind.Integer, count.toString(),
            "The number of buckets to use.");
        input.min = "1";
        input.max = max.toString();
        input.required = true;
        this.setCacheTitle("BucketDialog");
    }

    public getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}
