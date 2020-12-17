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
    BucketsInfo,
    EigenVal,
    IColumnDescription, RecordOrder,
    RemoteObjectId, Groups
} from "../javaBridge";
import {OnCompleteReceiver} from "../rpc";
import {BaseReceiver, TableTargetAPI} from "../modules";
import {IDataView} from "../ui/dataview";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {assert, ICancellable, significantDigits} from "../util";
import {AxisData} from "./axisData";
import {CorrelationMatrixReceiver, TableView} from "../modules";
import {ChartView} from "../modules";
import {TableMeta} from "../ui/receiver";

/**
 * Receives the result of a PCA computation and plots the singular values
 */
export class SpectrumReceiver extends OnCompleteReceiver<EigenVal> {
    public specView: SpectrumView;
    public newPage: FullPage;
    public constructor(page: FullPage,
                       protected originator: TableTargetAPI,
                       protected remoteObjectId: RemoteObjectId,
                       protected meta: TableMeta,
                       protected colNames: string[],
                       operation: ICancellable<EigenVal>,
                       protected reusePage: boolean) {
        super(page, operation, "Singular Value Spectrum");
        if (this.reusePage)
            this.newPage = this.page;
        else
            this.newPage = this.page.dataset!.newPage(
                new PageTitle("Singular Value Spectrum", page.title.format), this.page);
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
        this.specView = new SpectrumView(this.remoteObjectId, this.meta, this.colNames, this.newPage);

        const ev: number [] = eVals.eigenValues;
        const histogram: Groups<number> = { perBucket: ev, perMissing: 0 };
        const icd: IColumnDescription = { kind: "Integer", name: "Singular Values" };
        const range: BucketsInfo = { min: -.5, max: ev.length - .5,
            presentCount: 0, missingCount: 0 };
        const axisData = new AxisData(icd, range, ev.length);
        this.specView.updateView("Spectrum", histogram, axisData);
        this.newPage.reportError("Showing top " + eVals.eigenValues.length + "/" + this.colNames.length +
            " singular values, Total Variance: " + significantDigits(eVals.totalVar) +
            ", Explained Variance: " + significantDigits(eVals.explainedVar));
        this.specView.updateCompleted(this.elapsedMilliseconds());
    }

    private computePCA(): void {
        const pcaDialog = new Dialog("Principal Component Analysis",
            "Projects a set of numeric columns to a smaller set of numeric columns while preserving the 'shape' " +
            " of the data as much as possible.");
        const components = pcaDialog.addTextField("numComponents", "Number of components", FieldKind.Integer, "2",
            "Number of dimensions to project to.  Must be an integer bigger than 1 and " +
            "smaller than" + this.colNames.length);
        components.min = "1";
        components.max = this.colNames.length.toString();
        components.required = true;
        const name = pcaDialog.addTextField("projectionName", "Name for Projected columns", FieldKind.String,
            "PCA",
            "The projected columns will appear with this name followed by a number starting from 0");
        name.required = true;
        pcaDialog.setCacheTitle("PCADialog");
        pcaDialog.setAction(() => {
            const numComponents: number | null = pcaDialog.getFieldValueAsInt("numComponents");
            const projectionName: string = pcaDialog.getFieldValue("projectionName");
            if (numComponents == null || numComponents < 1 || numComponents > this.colNames.length) {
                this.page.reportError("Number of components for PCA must be between 1 (incl.) " +
                    "and the number of selected columns, " + this.colNames.length + " (incl.). (" +
                    numComponents + " does not satisfy this.)");
                return;
            }
            const rr = this.originator.createCorrelationMatrixRequest(this.colNames, this.meta.rowCount, true);
            const newestPage = this.newPage.dataset!.newPage(new PageTitle("Table", "Spectrum view"), this.newPage);
            const table = new TableView(this.remoteObjectId, this.meta, newestPage);
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
export class SpectrumView extends ChartView<Groups<number>> {
    protected axisData: AxisData;
    protected title: string;
    protected plot: HistogramPlot;

    constructor(remoteObjectId: RemoteObjectId, meta: TableMeta,
                protected colNames: string[], page: FullPage) {
        super(remoteObjectId, meta, page, "SVD Spectrum");

        this.createDiv("chart");
        this.createDiv("summary");
    }

    // noinspection JSUnusedLocalSymbols
    protected showTrellis(colName: string): void { /* not used */ }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {});
        this.plot = new HistogramPlot(this.surface);
    }

    public export(): void {
        // TODO?
    }

    public updateView(title: string, h: Groups<number>, axisData: AxisData): void {
        this.createNewSurfaces();
        if (h == null) {
            this.page.reportError("No data to display");
            return;
        }

        this.axisData = axisData;
        this.title = title;
        this.data = h;
        this.plot.setHistogram({first: h, second: null }, 1,
            axisData, null, this.page.dataset!.isPrivate(), this.meta.rowCount);
        this.plot.draw();
        this.standardSummary();
        //this.summary.set("Columns: " + this.colNames.join(""), 0);
        this.summary!.display();
    }

    protected onMouseMove(): void {}

    public resize(): void {
        this.updateView(this.title, this.data, this.axisData);
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: SpectrumSerialization = {
            ...super.serialize(),
            colNames: this.colNames,
        };
        return result;
    }

    public refresh(): void {
        const rr = this.createSpectrumRequest(this.colNames, this.meta.rowCount, true);
        rr.invoke(new SpectrumReceiver(this.page, this, this.getRemoteObjectId()!,
            this.meta,  this.colNames, rr, true));
    }

    public static reconstruct(ser: SpectrumSerialization, page: FullPage): IDataView | null {
        const colNames: string[] = ser.colNames;
        const args = this.validateSerialization(ser);
        if (colNames == null || args == null)
            return null;
        return new SpectrumView(ser.remoteObjectId, args, colNames, page);
    }

    // noinspection JSUnusedLocalSymbols
    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        assert(false);  // not used
    }

    // noinspection JSUnusedLocalSymbols
    public combine(how: CombineOperators): void {
        // not used
    }
}
