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

import {drag as d3drag} from "d3-drag";
import {mouse as d3mouse, select as d3select} from "d3-selection";
import {
    BasicColStats, CategoricalValues, ColumnAndRange, CombineOperators, HeatMap, Histogram2DArgs,
    RemoteObjectId, TableSummary,
} from "../javaBridge";
import {OnCompleteReceiver, Receiver, RpcRequest} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, BigTableView, TableTargetAPI} from "../tableTarget";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {PlottingSurface} from "../ui/plottingSurface";
import {Point, PointSet, Resolution} from "../ui/ui";
import {clamp, ICancellable, Pair, PartialResult, Seed} from "../util";
import {TableView} from "./tableView";
import {TrellisPlotDialog} from "./trellisHeatMapView";

/**
 * This class displays the results of performing a local affine multi-dimensional projection.
 * See the paper Local Affine Multidimensional Projection from IEEE Transactions on Visualization
 * and Computer Graphics, vol 17, issue 12, Dec 2011, by Paulo Joia, Danilo Coimbra, Jose A Cuminato,
 * Fernando V Paulovich, and Luis G Nonato.
 */
class LampView extends BigTableView {
    private minX: number;
    private minY: number;
    private maxX: number;
    private maxY: number;
    private maxVal: number; // Maximum value in the heatmap
    private heatMapCanvas: any;
    private heatMapChart: any;
    private controlPointsCanvas: any;
    private controlPointsChart: any;
    public controlPoints: PointSet;
    private heatMapDots: any[];
    private xDots: number;
    private yDots: number;
    private lampTableObject: TableTargetAPI;
    private colorLegend: HeatmapLegendPlot;
    private readonly lampColNames: string[];
    private readonly legendSurface: PlottingSurface;

    constructor(private tableObject: TableTargetAPI, rowCount: number, schema: SchemaClass,
                page: FullPage, private controlPointsId: RemoteObjectId,
                private selectedColumns: string[]) {
        super(tableObject.remoteObjectId, rowCount, schema, page, "LAMP");
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");
        this.topLevel.classList.add("lampView");

        const menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                },
                { text: "update ranges",
                    action: () => { this.fetchNewRanges(); },
                    help: "Redraw this view such that all data fits on screen.",
                },
                { text: "table",
                    action: () => { this.showTable(); },
                    help: "Show the data underlying this view using a table.",
                },
                { text: "3D heatmap...",
                    action: () => { this.heatMap3D(); },
                    help: "Specify a categorical column and replot this data" +
                    " grouped on values of that column.",
                },
            ]) },
        ]);
        this.page.setMenu(menu);

        this.legendSurface = new PlottingSurface(this.topLevel, page);
        // this.legendSurface.setMargins(0, 0, 0, 0);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
        this.colorLegend.setColorMapChangeEventListener(() => {
            this.refresh();
        });
        const chartDiv = document.createElement("div");
        this.topLevel.appendChild(chartDiv);

        const canvasSize = Math.min(
            PlottingSurface.getDefaultCanvasSize(this.getPage()).width,
            PlottingSurface.getDefaultCanvasSize(this.getPage()).height);
        const chartSize = Math.min(
            PlottingSurface.getDefaultChartSize(this.getPage()).width,
            PlottingSurface.getDefaultChartSize(this.getPage()).height);
        this.heatMapCanvas = d3select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "heatMap");
        this.heatMapChart = this.heatMapCanvas.append("g")
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.controlPointsCanvas = d3select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "controlPoints");
        this.controlPointsChart = this.controlPointsCanvas.append("g")
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        page.setDataView(this);

        this.lampColNames = [
            this.schema.uniqueColumnName("LAMP1"),
            this.schema.uniqueColumnName("LAMP2"),
        ];
    }

    public combine(how: CombineOperators): void {
        // not used
    }

    public refresh() {
        const canvasSize = Math.min(PlottingSurface.getDefaultCanvasSize(
            this.getPage()).width, PlottingSurface.getDefaultCanvasSize(this.getPage()).height);
        const chartSize = Math.min(PlottingSurface.getDefaultChartSize(
            this.getPage()).width, PlottingSurface.getDefaultChartSize(this.getPage()).height);
        this.controlPointsCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize);
        this.controlPointsChart
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.heatMapCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize);
        this.heatMapChart
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.updateControlPointsView();
        this.updateHeatMapView();
    }

    public updateRemoteTable(table: TableTargetAPI) {
        this.lampTableObject = table;
    }

    public updateControlPoints(pointSet: PointSet) {
        this.controlPoints = pointSet;
        /* Set the coordinate system of the plot */
        const [minX, maxX] = [Math.min(...this.controlPoints.points.map((p) => p.x)),
            Math.max(...this.controlPoints.points.map((p) => p.x))];
        const [minY, maxY] = [Math.min(...this.controlPoints.points.map((p) => p.y)),
            Math.max(...this.controlPoints.points.map((p) => p.y))];
        const maxRange = Math.max(maxX - minX, maxY - minY);
        this.minX = minX;
        this.minY = minY;
        this.maxX = minX + maxRange;
        this.maxY = minY + maxRange;

        this.updateControlPointsView();
        this.applyLAMP();
    }

    public updateHeatMap(heatMap: HeatMap, lampTime: number) {
        this.getPage().reportError(`LAMP completed in ${lampTime} s.`);

        this.xDots = heatMap.buckets.length;
        this.yDots = heatMap.buckets[0].length;
        this.heatMapDots = [];
        this.maxVal = 0;
        for (let i = 0; i < this.xDots; i++) {
            for (let j = 0; j < this.yDots; j++) {
                const val = heatMap.buckets[i][j];
                if (val > 0) {
                    this.heatMapDots.push({x: i / this.xDots, y: 1 - (j + 1) / this.yDots, v: val});
                    this.maxVal = Math.max(val, this.maxVal);
                }
            }
        }
        this.colorLegend.setData(1, this.maxVal);
        this.updateHeatMapView();
    }

    public updateHeatMapView() {
        if (this.heatMapDots == null)
            return;
        const chartWidth = this.heatMapChart.attr("width");
        const chartHeight = this.heatMapChart.attr("height");

        this.colorLegend.clear();
        this.heatMapChart.selectAll("*").remove();
        this.heatMapChart.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", chartWidth)
            .attr("height", chartHeight)
            .style("fill", "none")
            .style("stroke", "black");

        this.colorLegend.draw();
        this.heatMapChart.selectAll()
            .data(this.heatMapDots)
            .enter()
            .append("rect")
            .attr("x", (d) => d.x * chartWidth)
            .attr("y", (d) => d.y * chartHeight)
            .attr("data-val", (d) => d.v)
            .attr("width", chartWidth / this.xDots)
            .attr("height", chartHeight / this.yDots)
            .style("stroke-width", 0)
            .style("fill", (d) => this.colorLegend.getColor(d.v));
    }

    public applyLAMP() {
        const xBuckets = Math.ceil(this.heatMapChart.attr("width") / Resolution.minDotSize);
        const yBuckets = Math.ceil(this.heatMapChart.attr("height") / Resolution.minDotSize);
        const xColAndRange: ColumnAndRange = {
            min: this.minX,
            max: this.maxX,
            columnName: this.lampColNames[0],
            bucketBoundaries: null,
        };
        const yColAndRange: ColumnAndRange = {
            min: this.minY,
            max: this.maxY,
            columnName: this.lampColNames[1],
            bucketBoundaries: null,
        };
        const arg: Histogram2DArgs = {
            first: xColAndRange,
            second: yColAndRange,
            samplingRate: 1.0,  // TODO
            seed: Seed.instance.get(),
            xBucketCount: xBuckets,
            yBucketCount: yBuckets,
            cdfBucketCount: 0,
            cdfSamplingRate: 1.0,
        };
        const rr = this.tableObject.createLAMPMapRequest(
            this.controlPointsId, this.selectedColumns, this.controlPoints, this.lampColNames);
        rr.invoke(new LAMPMapReceiver(this.page, rr, this, arg));
    }

    public updateRanges(l1: BasicColStats, l2: BasicColStats) {
        const [cpMinX, cpMaxX] = [Math.min(...this.controlPoints.points.map((p) => p.x)),
            Math.max(...this.controlPoints.points.map((p) => p.x))];
        const [cpMinY, cpMaxY] = [Math.min(...this.controlPoints.points.map((p) => p.y)),
            Math.max(...this.controlPoints.points.map((p) => p.y))];
        this.minX = Math.min(l1.min, cpMinX);
        this.minY = Math.min(l2.min, cpMinY);
        const maxX = Math.max(l1.max, cpMaxX);
        const maxY = Math.max(l2.max, cpMaxY);
        const range = Math.max(maxX - this.minX, maxY - this.minY);
        this.maxX = this.minX + range;
        this.maxY = this.minY + range;
        this.refresh();
        this.heatMapChart.selectAll("*").remove();
        this.applyLAMP();
    }

    private fetchNewRanges() {
        const l1: CategoricalValues = new CategoricalValues("LAMP1");
        const l2: CategoricalValues = new CategoricalValues("LAMP2");
        const rr = this.lampTableObject.createRange2DRequest(l1, l2);
        rr.invoke(new LAMPRangeCollector(this.page, rr, this));
    }

    private updateControlPointsView() {
        this.colorLegend.clear();
        if (this.controlPoints == null)
            return; // Control points are not yet set.
        this.controlPointsChart.selectAll("*").remove();
        this.controlPointsChart.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.controlPointsChart.attr("width"))
            .attr("height", this.controlPointsChart.attr("height"))
            .style("fill", "none")
            .style("stroke", "black");
        const chartSize = this.controlPointsChart.attr("width");
        const range = Math.max(this.maxX - this.minX, this.maxY - this.minY);
        const scale = chartSize / range;

        const plot = this.controlPointsChart.append("g")
            .attr("transform", `translate(0, ${this.controlPointsChart.attr("height")}) scale(1, -1) scale(${scale}) " +
            "translate(${-this.minX}, ${-this.minY})`);

        const radius = 4;
        plot.selectAll("points")
            .data(this.controlPoints.points)
            .enter().append("circle")
                .attr("cx", (p) => p.x)
                .attr("cy", (p) => p.y)
                .attr("r", radius / scale)
                .attr("class", "controlPoint")
                .attr("stroke", "black")
                .attr("vector-effect", "non-scaling-stroke")
                .call(d3drag()
                    .on("drag", (p: Point, i: number, circles: Element[]) => {
                        const mouse = d3mouse(plot.node());
                        mouse[0] = clamp(mouse[0], this.minX, this.minX + range);
                        mouse[1] = clamp(mouse[1], this.minY, this.minY + range);
                        p.x = mouse[0];
                        p.y = mouse[1];
                        d3select(circles[i])
                            .attr("cx", clamp(mouse[0], this.minX, this.minX + range))
                            .attr("cy", clamp(mouse[1], this.minY, this.minY + range));
                    })
                    .on("end", (/*p: Point, i: number, circles: Element[]*/) => {
                        this.applyLAMP();
                    }),
                );
    }

    private showTable() {
        const page = this.dataset.newPage("Table", this.page);
        const table = new TableView(
            this.lampTableObject.remoteObjectId, this.rowCount, this.schema, page);
        page.setDataView(table);
        const rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new SchemaCollector(this.getPage(), rr, this.schema, this.lampTableObject, this.lampColNames));
    }

    private heatMap3D() {
        // The lamp table has a new schema, so we have to retrieve it.
        const rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new SchemaCollector(this.getPage(), rr, this.schema, this.lampTableObject, this.lampColNames));
    }
}

/**
 * Displays a dialog to allow the user to select the parameters of a LAMP multi-dimensional
 * projection.
 */
export class LAMPDialog extends Dialog {
    public static maxNumSamples = 100;

    constructor(private selectedColumns: string[], private page: FullPage,
                private rowCount: number,
                private schema: SchemaClass, private remoteObject: TableView) {
        super("LAMP", "Computes a 2D projection of the data based on a set of " +
            "control-points that the user can control.");
        const sel = this.addSelectField("controlPointSelection", "Control point selection",
            ["Random samples", "Category centroids"], "Random samples",
            "The method used to select the control points.");
        sel.onchange = () => this.ctrlPointsChanged();
        this.addTextField("numSamples", "No. control points", FieldKind.Integer, "5",
            "The number of control points to select.");
        const catColumns = [""];
        for (let i = 0; i < schema.length; i++)
            if (schema.get(i).kind === "Category")
                catColumns.push(schema.get(i).name);
        this.addSelectField("category", "Category for centroids", catColumns, "",
            "A column name with categorical data that will be used to defined the control points." +
            "There will be one control point for each categorical value.");
        this.addSelectField("controlPointProjection", "Control point projection", ["MDS"], "MDS",
            "The projection technique.  Currently only Multidimensional Scaling is an option.");
        this.setAction(() => this.execute());
        this.setCacheTitle("LAMPDialog");
        this.ctrlPointsChanged();
    }

    private ctrlPointsChanged(): void {
        const sel = this.getFieldValue("controlPointSelection");
        switch (sel) {
            case "Random samples":
                this.showField("numSamples", true);
                this.showField("category", false);
                break;
            case "Category centroids":
                this.showField("numSamples", false);
                this.showField("category", true);
                break;
        }
    }

    private execute() {
        const numSamples = this.getFieldValueAsInt("numSamples");
        const selection = this.getFieldValue("controlPointSelection");
        const projection = this.getFieldValue("controlPointProjection");
        const category = this.getFieldValue("category");
        let rr: RpcRequest<PartialResult<RemoteObjectId>>;
        switch (selection) {
            case "Random samples": {
                if (numSamples > LAMPDialog.maxNumSamples) {
                    this.page.reportError(`Too many samples. Use at most ${LAMPDialog.maxNumSamples}.`);
                    return;
                }
                rr = this.remoteObject.createSampledControlPointsRequest(
                    this.remoteObject.getTotalRowCount(), numSamples, this.selectedColumns);
                break;
            }
            case "Category centroids": {
                if (category === "") {
                    this.page.reportError("No category selected for centroids.");
                    return;
                }
                rr = this.remoteObject.createCategoricalCentroidsControlPointsRequest(category, this.selectedColumns);
                break;
            }
            default: {
                this.page.reportError(`${selection} is not implemented.`);
            }
        }

        const newPage = this.page.dataset.newPage("LAMP", this.page);
        switch (projection) {
            case "MDS": {
                rr.invoke(new ControlPointsProjector(
                    newPage, rr, this.remoteObject, this.selectedColumns,
                    this.rowCount, this.schema));
                break;
            }
            default: {
                this.page.reportError(`${projection} is not implemented.`);
            }
        }
    }
}

class ControlPointsProjector extends BaseRenderer {
    constructor(page: FullPage,
                operation: ICancellable,
                private tableObject: TableTargetAPI,
                private selectedColumns: string[],
                private rowCount: number,
                private schema: SchemaClass) {
        super(page, operation, "Sampling control points", page.dataset);
    }

    public run() {
        super.run();
        const rr = this.tableObject.createMDSProjectionRequest(this.remoteObject.remoteObjectId);
        rr.invoke(new ControlPointsRenderer(
            this.page, rr, this.tableObject, this.rowCount, this.schema,
            this.remoteObject.remoteObjectId,
            this.selectedColumns));
    }
}

class ControlPointsRenderer extends Receiver<PointSet> {
    private controlPointsView: LampView;
    private points: PointSet;

    constructor(page: FullPage, operation: ICancellable,
                tableObject: TableTargetAPI, rowCount: number, schema: SchemaClass,
                controlPointsId: RemoteObjectId, private selectedColumns: string[]) {
        super(page, operation, "Projecting control points");
        this.controlPointsView = new LampView(
            tableObject, rowCount, schema, page, controlPointsId, this.selectedColumns);
    }

    public onNext(result: PartialResult<PointSet>) {
        super.onNext(result);
        this.points = result.data;
        if (this.points != null)
            this.controlPointsView.updateControlPoints(this.points);
    }
}

class LAMPMapReceiver extends BaseRenderer {
    constructor(page: FullPage, operation: ICancellable, private cpView: LampView,
                private arg: Histogram2DArgs) {
        super(page, operation, "Computing LAMP", cpView.dataset);
    }

    public run() {
        super.run();
        const lampTime = this.elapsedMilliseconds() / 1000;
        this.cpView.updateRemoteTable(this.remoteObject);
        const rr = this.remoteObject.createHeatMapRequest(this.arg);
        rr.invoke(new LAMPHeatMapReceiver(this.page, rr, this.cpView, lampTime));
    }
}

class LAMPHeatMapReceiver extends Receiver<HeatMap> {
    private heatMap: HeatMap;
    constructor(page: FullPage, operation: ICancellable,
                private controlPointsView: LampView, private lampTime: number) {
        super(page, operation, "Computing heatmap");
    }

    public onNext(result: PartialResult<HeatMap>) {
        super.onNext(result);
        this.heatMap = result.data;
        if (this.heatMap != null)
            this.controlPointsView.updateHeatMap(this.heatMap, this.lampTime);
    }
}

class LAMPRangeCollector extends Receiver<Pair<BasicColStats, BasicColStats>> {
    constructor(page: FullPage, operation: ICancellable, private cpView: LampView) {
        super(page, operation, "Getting LAMP ranges.");
    }

    public onNext(result: PartialResult<Pair<BasicColStats, BasicColStats>>): void {
        super.onNext(result);
        this.cpView.updateRanges(result.data.first, result.data.second);
    }
}

class SchemaCollector extends OnCompleteReceiver<TableSummary> {
    constructor(page: FullPage, operation: ICancellable,
                private schema: SchemaClass,
                private tableObject: TableTargetAPI,
                private lampColumnNames: string[]) {
        super(page, operation, "Getting new schema");
    }

    public run(): void {
        if (this.value == null)
            return;
        const newSchema = new SchemaClass(this.value.schema);
        newSchema.copyDisplayNames(this.schema);
        const dialog = new TrellisPlotDialog(
            this.lampColumnNames, this.page, this.value.rowCount, newSchema, this.tableObject, true);
        dialog.show();
    }
}
