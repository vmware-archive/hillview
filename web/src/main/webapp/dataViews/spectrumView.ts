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

import {IViewSerialization, SpectrumSerialization} from "../datasetView";
import {
    CombineOperators,
    DataRange,
    EigenVal,
    HistogramBase,
    IColumnDescription, RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {OnCompleteReceiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BigTableView, TableTargetAPI} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {ICancellable, significantDigits} from "../util";
import {AxisData} from "./axisData";
import {CorrelationMatrixReceiver, TableView} from "./tableView";
/**
 * Receives the result of a PCA computation and plots the singular values
 */
export class SpectrumReceiver extends OnCompleteReceiver<EigenVal> {
    public specView: SpectrumView;
    public newPage: FullPage;
    public constructor(page: FullPage,
                       protected originator: TableTargetAPI,
                       protected remoteObjectId: RemoteObjectId,
                       protected rowCount: number,
                       protected schema: SchemaClass,
                       protected colNames: string[],
                       operation: ICancellable<EigenVal>,
                       protected reusePage: boolean) {
        super(page, operation, "Singular Value Spectrum");
        if (this.reusePage)
            this.newPage = this.page;
        else
            this.newPage = this.page.dataset.newPage("Singular Value Spectrum", this.page);
    }

    public run(eVals: EigenVal): void {
        const menu = new SubMenu([]);
        menu.addItem({
                text: "PCA...",
                action: () => this.computePCA(),
                help: "Perform Principal Component Analysis on a set of numeric columns. " +
                "This produces a smaller set of columns that preserve interesting properties of the data.",
            },
            true);
        const topMenu = new TopMenu([{ text: "View", help: "Change the way the data is displayed.", subMenu: menu}]);
        this.newPage.setMenu(topMenu);
        this.specView = new SpectrumView(this.remoteObjectId, this.rowCount, this.colNames, this.schema, this.newPage);
        this.newPage.setDataView(this.specView);

        const ev: number [] = eVals.eigenValues;
        const histogram: HistogramBase = { buckets: ev, missingData: 0 };
        const icd: IColumnDescription = { kind: "Integer", name: "Singular Values" };
        const range: DataRange = { min: -.5, max: ev.length - .5,
            presentCount: 0, missingCount: 0 };
        const axisData = new AxisData(icd, range);
        this.specView.updateView("Spectrum", histogram, axisData, this.elapsedMilliseconds());
        this.newPage.reportError("Showing top " + eVals.eigenValues.length + "/" + this.colNames.length +
            " singular values, Total Variance: " + significantDigits(eVals.totalVar) +
            ", Explained Variance: " + significantDigits(eVals.explainedVar));
    }

    private computePCA() {
        const pcaDialog = new Dialog("Principal Component Analysis",
            "Projects a set of numeric columns to a smaller set of numeric columns while preserving the 'shape' " +
            " of the data as much as possible.");
        pcaDialog.addTextField("numComponents", "Number of components", FieldKind.Integer, "2",
            "Number of dimensions to project to.  Must be an integer bigger than 1 and " +
            "smaller than" + this.colNames.length);
        pcaDialog.addTextField("projectionName", "Name for Projected columns", FieldKind.String,
            "PCA",
            "The projected columns will appear with this name followed by a number starting from 0");
        pcaDialog.setCacheTitle("PCADialog");
        pcaDialog.setAction(() => {
            const numComponents: number = pcaDialog.getFieldValueAsInt("numComponents");
            const projectionName: string = pcaDialog.getFieldValue("projectionName");
            if (numComponents < 1 || numComponents > this.colNames.length) {
                this.page.reportError("Number of components for PCA must be between 1 (incl.) " +
                    "and the number of selected columns, " + this.colNames.length + " (incl.). (" +
                    numComponents + " does not satisfy this.)");
                return;
            }
            const rr = this.originator.createCorrelationMatrixRequest(this.colNames, this.rowCount, true);
            const newestPage = this.newPage.dataset.newPage("Table", this.newPage);
            const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newestPage);
            newestPage.setDataView(table);
            const order  = new RecordOrder([]);
            rr.invoke(new CorrelationMatrixReceiver(newestPage, table, rr, order, numComponents, projectionName));
        });
        pcaDialog.show();
    }
}

/**
 * A SpectrumView plots a one-dimensional bar-chart showing the top singular values.
 */
export class SpectrumView extends BigTableView {
    protected currentData: {
        histogram: HistogramBase,
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
        this.surface = new HtmlPlottingSurface(this.chartDiv, page);
        this.plot = new HistogramPlot(this.surface);
        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    public updateView(title: string, h: HistogramBase,
                      axisData: AxisData, elapsedMs: number): void {
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

    public serialize(): IViewSerialization {
        const result: SpectrumSerialization = {
            ...super.serialize(),
            colNames: this.colNames,
        };
        return result;
    }

    public static reconstruct(ser: SpectrumSerialization, page: FullPage): IDataView {
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const colNames: string[] = ser.colNames;
        if (colNames == null || schema == null)
            return null;
        const sv = new SpectrumView(ser.remoteObjectId, ser.rowCount, colNames, schema, page);
        const rr = sv.createSpectrumRequest(colNames, ser.rowCount, true);
        rr.invoke(new SpectrumReceiver(page, sv, ser.remoteObjectId,
            ser.rowCount, schema,  colNames, rr, true));
        return sv;
    }

    public combine(how: CombineOperators): void {
        // not used
    }
}
