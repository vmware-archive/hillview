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

import {d3} from "../ui/d3-modules";
import {Dialog, FieldKind} from "../ui/dialog";
import {TopMenu, SubMenu} from "../ui/menu";
import {
    RangeInfo, BasicColStats, Schema, RecordOrder, ColumnAndRange, Histogram2DArgs, TableSummary, RemoteObjectId,
    HeatMap
} from "../javaBridge";
import {Renderer, RpcRequest, OnCompleteRenderer} from "../rpc";
import {PartialResult, clamp, Pair, ICancellable, Seed} from "../util";
import {Point, PointSet, Resolution} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {TableView, NextKReceiver} from "./tableView";
import {HeatMapArrayDialog} from "./trellisHeatMapView";
import {RemoteTableObject, RemoteTableObjectView, RemoteTableRenderer} from "../tableTarget";
import {PlottingSurface} from "../ui/plottingSurface";

/**
 * This class displays the results of performing a local affine multi-dimensional projection.
 * See the paper Local Affine Multidimensional Projection from IEEE Transactions on Visualization
 * and Computer Graphics, vol 17, issue 12, Dec 2011, by Paulo Joia, Danilo Coimbra, Jose A Cuminato,
 * Fernando V Paulovich, and Luis G Nonato.
 */
class LampView extends RemoteTableObjectView {
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
    private heatMapDots: Array<any>;
    private xDots: number;
    private yDots: number;
    private lampTableObject: RemoteTableObject;
    private colorLegend: HeatmapLegendPlot;
    private lampColNames: string[];
    private legendSurface: PlottingSurface;

    constructor(private tableObject: RemoteTableObject, private originalSchema,
                page: FullPage, private controlPointsId, private selectedColumns) {
        super(tableObject.remoteObjectId, tableObject.originalTableId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");
        this.topLevel.classList.add("lampView");

        let menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view."
                },
                { text: "update ranges",
                    action: () => { this.fetchNewRanges(); },
                    help: "Redraw this view such that all data fits on screen."
                },
                { text: "table",
                    action: () => { this.showTable(); },
                    help: "Show the data underlying this view using a table."
                },
                { text: "3D heatmap...",
                    action: () => { this.heatMap3D(); },
                    help: "Specify a categorical column and replot this data" +
                    " grouped on values of that column."
                },
            ]) }
        ]);
        this.page.setMenu(menu);

        this.legendSurface = new PlottingSurface(this.topLevel, page);
        //this.legendSurface.setMargins(0, 0, 0, 0);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
        this.colorLegend.setColorMapChangeEventListener(() => {
            this.refresh();
        });
        let chartDiv = document.createElement("div");
        this.topLevel.appendChild(chartDiv);

        let canvasSize = Math.min(
            PlottingSurface.getDefaultCanvasSize(this.getPage()).width,
            PlottingSurface.getDefaultCanvasSize(this.getPage()).height);
        let chartSize = Math.min(
            PlottingSurface.getDefaultChartSize(this.getPage()).width,
            PlottingSurface.getDefaultChartSize(this.getPage()).height);
        this.heatMapCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "heatMap");
        this.heatMapChart = this.heatMapCanvas.append("g")
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.controlPointsCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "controlPoints");
        this.controlPointsChart = this.controlPointsCanvas.append("g")
            .attr("transform", `translate(${PlottingSurface.leftMargin}, ${PlottingSurface.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        page.setDataView(this);

        this.lampColNames = [
            TableView.uniqueColumnName(this.originalSchema, "LAMP1"),
            TableView.uniqueColumnName(this.originalSchema, "LAMP2")
        ];
    }

    public refresh() {
        let canvasSize = Math.min(PlottingSurface.getDefaultCanvasSize(this.getPage()).width, PlottingSurface.getDefaultCanvasSize(this.getPage()).height);
        let chartSize = Math.min(PlottingSurface.getDefaultChartSize(this.getPage()).width, PlottingSurface.getDefaultChartSize(this.getPage()).height);
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

    public updateRemoteTable(table: RemoteTableObject) {
        this.lampTableObject = table;
    }

    public updateControlPoints(pointSet: PointSet) {
        this.controlPoints = pointSet;
        /* Set the coordinate system of the plot */
        let [minX, maxX] = [Math.min(...this.controlPoints.points.map(p => p.x)), Math.max(...this.controlPoints.points.map(p => p.x))];
        let [minY, maxY] = [Math.min(...this.controlPoints.points.map(p => p.y)), Math.max(...this.controlPoints.points.map(p => p.y))];
        let maxRange = Math.max(maxX - minX, maxY - minY);
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
                let val = heatMap.buckets[i][j];
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
        let chartWidth = this.heatMapChart.attr("width");
        let chartHeight = this.heatMapChart.attr("height");

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
            .attr("x", d => d.x * chartWidth)
            .attr("y", d => d.y * chartHeight)
            .attr("data-val", d => d.v)
            .attr("width", chartWidth / this.xDots)
            .attr("height", chartHeight / this.yDots)
            .style("stroke-width", 0)
            .style("fill", d => this.colorLegend.getColor(d.v));
    }

    public applyLAMP() {
        let xBuckets = Math.ceil(this.heatMapChart.attr("width") / Resolution.minDotSize);
        let yBuckets = Math.ceil(this.heatMapChart.attr("height") / Resolution.minDotSize);
        let xColAndRange: ColumnAndRange = {
            min: this.minX,
            max: this.maxX,
            columnName: this.lampColNames[0],
            bucketBoundaries: null
        };
        let yColAndRange: ColumnAndRange = {
            min: this.minY,
            max: this.maxY,
            columnName: this.lampColNames[1],
            bucketBoundaries: null
        };
        let arg: Histogram2DArgs = {
            first: xColAndRange,
            second: yColAndRange,
            samplingRate: 1.0,  // TODO
            seed: Seed.instance.get(),
            xBucketCount: xBuckets,
            yBucketCount: yBuckets,
            cdfBucketCount: 0,
            cdfSamplingRate: 1.0
        };
        let rr = this.tableObject.createLAMPMapRequest(this.controlPointsId, this.selectedColumns, this.controlPoints, this.lampColNames);
        rr.invoke(new LAMPMapReceiver(this.page, rr, this, arg));
    }

    public updateRanges(l1: BasicColStats, l2: BasicColStats) {
        let [cpMinX, cpMaxX] = [Math.min(...this.controlPoints.points.map(p => p.x)), Math.max(...this.controlPoints.points.map(p => p.x))];
        let [cpMinY, cpMaxY] = [Math.min(...this.controlPoints.points.map(p => p.y)), Math.max(...this.controlPoints.points.map(p => p.y))];
        this.minX = Math.min(l1.min, cpMinX);
        this.minY = Math.min(l2.min, cpMinY);
        let maxX = Math.max(l1.max, cpMaxX);
        let maxY = Math.max(l2.max, cpMaxY);
        let range = Math.max(maxX - this.minX, maxY - this.minY);
        this.maxX = this.minX + range;
        this.maxY = this.minY + range;
        this.refresh();
        this.heatMapChart.selectAll("*").remove();
        this.applyLAMP();
    }

    private fetchNewRanges() {
        let l1: RangeInfo = new RangeInfo("LAMP1");
        let l2: RangeInfo = new RangeInfo("LAMP2");
        let rr = this.lampTableObject.createRange2DRequest(l1, l2);
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
        let chartSize = this.controlPointsChart.attr("width");
        let range = Math.max(this.maxX - this.minX, this.maxY - this.minY);
        let scale = chartSize / range;

        let plot = this.controlPointsChart.append("g")
            .attr("transform", `translate(0, ${this.controlPointsChart.attr("height")}) scale(1, -1) scale(${scale}) translate(${-this.minX}, ${-this.minY})`);

        let radius = 4;
        plot.selectAll("points")
            .data(this.controlPoints.points)
            .enter().append("circle")
                .attr("cx", p => p.x)
                .attr("cy", p => p.y)
                .attr("r", radius / scale)
                .attr("class", "controlPoint")
                .attr("stroke", "black")
                .attr("vector-effect", "non-scaling-stroke")
                .call(d3.drag()
                    .on("drag", (p: Point, i: number, circles: Element[]) => {
                        let mouse = d3.mouse(plot.node());
                        mouse[0] = clamp(mouse[0], this.minX, this.minX + range);
                        mouse[1] = clamp(mouse[1], this.minY, this.minY + range);
                        p.x = mouse[0];
                        p.y = mouse[1];
                        d3.select(circles[i])
                            .attr("cx", clamp(mouse[0], this.minX, this.minX + range))
                            .attr("cy", clamp(mouse[1], this.minY, this.minY + range));
                    })
                    .on("end", (/*p: Point, i: number, circles: Element[]*/) => {
                        this.applyLAMP();
                    })
                )
    }

    private showTable() {
        let page = new FullPage("Table", "Table", this.page);
        this.getPage().insertAfterMe(page);
        let table = new TableView(this.lampTableObject.remoteObjectId, this.lampTableObject.originalTableId, page);
        page.setDataView(table);
        let rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new NextKReceiver(this.page, table, rr, false, new RecordOrder([])));
    }

    private heatMap3D() {
        // The lamp table has a new schema, so we have to retrieve it.
        let rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new SchemaCollector(this.getPage(), rr, this.lampTableObject, this.lampColNames));
    }
}

/**
 * Displays a dialog to allow the user to select the parameters of a LAMP multi-dimensional
 * projection.
 */
export class LAMPDialog extends Dialog {
    public static maxNumSamples = 100;

    constructor(private selectedColumns: string[], private page: FullPage,
                private schema: Schema, private remoteObject: TableView) {
        super("LAMP", "Computes a 2D projection of the data based on a set of control-points that the user can control.");
        let sel = this.addSelectField("controlPointSelection", "Control point selection",
            ["Random samples", "Category centroids"], "Random samples",
            "The method used to select the control points.");
        sel.onchange = () => this.ctrlPointsChanged();
        this.addTextField("numSamples", "No. control points", FieldKind.Integer, "5",
            "The number of control points to select.");
        let catColumns = [""];
        for (let i = 0; i < schema.length; i++)
            if (schema[i].kind == "Category")
                catColumns.push(schema[i].name);
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
        let sel = this.getFieldValue("controlPointSelection");
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
        let numSamples = this.getFieldValueAsInt("numSamples");
        let selection = this.getFieldValue("controlPointSelection");
        let projection = this.getFieldValue("controlPointProjection");
        let category = this.getFieldValue("category");
        let rr: RpcRequest<PartialResult<RemoteObjectId>>;
        switch (selection) {
            case "Random samples": {
                if (numSamples > LAMPDialog.maxNumSamples) {
                    this.page.reportError(`Too many samples. Use at most ${LAMPDialog.maxNumSamples}.`);
                    return;
                }
                rr = this.remoteObject.createSampledControlPointsRequest(this.remoteObject.getTotalRowCount(), numSamples, this.selectedColumns);
                break;
            }
            case "Category centroids": {
                if (category == "") {
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

        let newPage = new FullPage("LAMP", "LAMP", this.page);
        this.page.insertAfterMe(newPage);

        switch (projection) {
            case "MDS": {
                rr.invoke(new ControlPointsProjector(
                    newPage, rr, this.remoteObject, this.selectedColumns, this.schema));
                break;
            }
            default: {
                this.page.reportError(`${projection} is not implemented.`);
            }
        }
    }
}

class ControlPointsProjector extends RemoteTableRenderer {
    constructor(page, operation, private tableObject: RemoteTableObject, private selectedColumns, private schema) {
        super(page, operation, "Sampling control points", tableObject.originalTableId);
    }

    run() {
        super.run();
        let rr = this.tableObject.createMDSProjectionRequest(this.remoteObject.remoteObjectId);
        rr.invoke(new ControlPointsRenderer(
            this.page, rr, this.tableObject, this.schema, this.remoteObject.remoteObjectId, this.selectedColumns));
    }
}

class ControlPointsRenderer extends Renderer<PointSet> {
    private controlPointsView: LampView;
    private points: PointSet;

    constructor(page, operation, tableObject, schema, controlPointsId, private selectedColumns) {
        super(page, operation, "Projecting control points");
        this.controlPointsView = new LampView(
            tableObject, schema, page, controlPointsId, this.selectedColumns);
    }

    public onNext(result: PartialResult<PointSet>) {
        super.onNext(result);
        this.points = result.data;
        if (this.points != null)
            this.controlPointsView.updateControlPoints(this.points);
    }
}

class LAMPMapReceiver extends RemoteTableRenderer {
    constructor(page: FullPage, operation: ICancellable, private cpView: LampView,
                private arg: Histogram2DArgs) {
        super(page, operation, "Computing LAMP", cpView.originalTableId);
    }

    run() {
        super.run();
        let lampTime = this.elapsedMilliseconds() / 1000;
        this.cpView.updateRemoteTable(this.remoteObject);
        let rr = this.remoteObject.createHeatMapRequest(this.arg);
        rr.invoke(new LAMPHeatMapReceiver(this.page, rr, this.cpView, lampTime));
    }
}

class LAMPHeatMapReceiver extends Renderer<HeatMap> {
    private heatMap: HeatMap;
    constructor(page: FullPage, operation: ICancellable,
                private controlPointsView: LampView, private lampTime: number) {
        super(page, operation, "Computing heatmap")
    }

    onNext(result: PartialResult<HeatMap>) {
        super.onNext(result);
        this.heatMap = result.data;
        if (this.heatMap != null)
            this.controlPointsView.updateHeatMap(this.heatMap, this.lampTime);
    }
}

class LAMPRangeCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    constructor(page: FullPage, operation: ICancellable, private cpView: LampView) {
        super(page, operation, "Getting LAMP ranges.")
    }

    onNext(result: PartialResult<Pair<BasicColStats, BasicColStats>>): void {
        super.onNext(result);
        this.cpView.updateRanges(result.data.first, result.data.second);
    }
}

class SchemaCollector extends OnCompleteRenderer<TableSummary> {
    constructor(page: FullPage, operation: ICancellable,
                private tableObject: RemoteTableObject, private lampColumnNames: string[]) {
        super(page, operation, "Getting new schema");
    }

    run(): void {
        if (this.value == null)
            return;
        let dialog = new HeatMapArrayDialog(
            this.lampColumnNames, this.page, this.value.schema, this.tableObject, true);
        dialog.show();
    }
}
