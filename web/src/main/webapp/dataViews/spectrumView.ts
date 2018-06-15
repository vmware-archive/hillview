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
    Histogram, CombineOperators, RemoteObjectId, BasicColStats, IColumnDescription, EigenVal
} from "../javaBridge";
import {TopMenu} from "../ui/menu";
import {ICancellable, significantDigits} from "../util";
import {FullPage} from "../ui/fullPage";
import {AxisData} from "./axisData";
import {BigTableView} from "../tableTarget";
import {HistogramPlot} from "../ui/histogramPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {OnCompleteReceiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {IDataView} from "../ui/dataview";
import {IViewSerialization} from "../datasetView";

/**
 * Receives the result of a PCA computation and plots the singular values
 */
export class SpectrumReceiver extends OnCompleteReceiver<EigenVal> {
    public specView: SpectrumView;
    public constructor(page: FullPage,
                       protected remoteObjectId: RemoteObjectId,
                       protected rowCount: number,
                       protected schema: SchemaClass,
                       protected colNames: string[],
                       operation: ICancellable,
                       protected reusePage: boolean) {
        super(page, operation, "Singular Value Spectrum");
    }

    run(eVals: EigenVal): void {
        let newPage: FullPage;
        if (this.reusePage)
            newPage = this.page;
        else
            newPage = this.page.dataset.newPage("Singular Value Spectrum", this.page);
        this.specView = new SpectrumView(
            this.remoteObjectId, this.rowCount, this.colNames,
            this.schema, newPage);
        newPage.setDataView(this.specView);

        let ev: number [] = eVals.eigenValues;
        let histogram: Histogram = { buckets: ev, missingData: 0, outOfRange: 0 };
        let icd: IColumnDescription = {kind: "Integer", name: "Singular Values" };
        let stats: BasicColStats = {momentCount: 0, min: -.5, max: ev.length - .5, moments: null, presentCount: 0, missingCount: 0};
        let axisData = new AxisData(icd, stats, null, ev.length);
        this.specView.updateView("Spectrum", histogram, axisData, this.elapsedMilliseconds());
        newPage.reportError("Showing top " + eVals.eigenValues.length + "/" + this.colNames.length +
            " singular values, Total Variance: " + significantDigits(eVals.totalVar) +
            ", Explained Variance: " + significantDigits(eVals.explainedVar));
    }
}

/**
 * A SpectrumView plots a one-dimensional bar-chart showing the top singular values.
 */
export class SpectrumView extends BigTableView {
    protected currentData: {
        histogram: Histogram,
        axisData: AxisData,
        title: string,
    };
    protected menu: TopMenu;
    protected plot: HistogramPlot;
    protected surface: PlottingSurface;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;

    constructor(remoteObjectId: RemoteObjectId, rowCount: number,
                protected colNames: string[],
                schema: SchemaClass, page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "SVD Spectrum");

        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);
        this.chartDiv.style.display = "flex";
        this.chartDiv.style.flexDirection = "column";
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new HistogramPlot(this.surface);
        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    public updateView(title: string, h: Histogram,
                      axisData: AxisData, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (h == null) {
            this.page.reportError("No data to display");
            return;
        }

        this.currentData = {
            axisData: axisData,
            title: title,
            histogram: h };

        this.plot.setHistogram(h, 1, axisData);
        this.plot.draw();

        this.summary.innerHTML = "Columns: " + this.colNames.join(", ");
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.title,
            this.currentData.histogram,
            this.currentData.axisData,
            0);
    }

    serialize(): IViewSerialization {
        let result = super.serialize();
        result["colNames"] = this.colNames;
        return result;
    }

    static reconstruct(ser: IViewSerialization, page: FullPage): IDataView {
        let schema = new SchemaClass([]).deserialize(ser.schema);
        let colNames: string[] = ser["colNames"];
        if (colNames == null || schema == null)
            return null;
        let sv = new SpectrumView(ser.remoteObjectId, ser.rowCount, colNames, schema, page);
        let rr = sv.createSpectrumRequest(colNames, ser.rowCount, true);
        rr.invoke(new SpectrumReceiver(page, ser.remoteObjectId,
            ser.rowCount, schema, colNames, rr, true));
        return sv;
    }

    public combine(how: CombineOperators): void {
        // not used
    }
}
