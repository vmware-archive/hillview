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

import {d3} from "./d3-modules";
import {Dialog} from "./dialog";
import {TopMenu, SubMenu} from "./menu";
import {NextKList, TableView, TableRenderer} from "./table";
import {
    RangeInfo, BasicColStats, Schema, RemoteTableObject, RemoteTableObjectView, RemoteTableRenderer, RecordOrder,
    ColumnAndRange, Histogram2DArgs
} from "./tableData";
import {FullPage, Resolution} from "./ui";
import {Renderer, RpcRequest} from "./rpc";
import {PartialResult, Point2D, clamp, Pair, ICancellable, Seed} from "./util";
import {HeatMapData} from "./heatMap";
import {HeatMapArrayDialog} from "./heatMapArray";
import {ColorMap, ColorLegend} from "./vis";

export class PointSet2D {
    points: Point2D[];
}

class ControlPointsView extends RemoteTableObjectView {
    private minX: number;
    private minY: number;
    private maxX: number;
    private maxY: number;
    private maxVal: number; // Maximum value in the heat map
    private heatMapCanvas: any;
    private heatMapChart: any;
    private controlPointsCanvas: any;
    private controlPointsChart: any;
    public controlPoints: PointSet2D;
    private heatMapDots: Array<any>;
    private xDots: number;
    private yDots: number;
    private lampTableObject: RemoteTableObject;
    private colorMap: ColorMap;
    private colorLegend: ColorLegend;
    private lampColNames: string[];

    constructor(private originalTableObject: RemoteTableObject, private originalSchema,
                page: FullPage, private controlPointsId, private selectedColumns) {
        super(originalTableObject.remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");
        this.topLevel.classList.add("lampView");

        let menu = new TopMenu( [
            { text: "View", subMenu: new SubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "update ranges", action: () => { this.fetchNewRanges(); } },
                { text: "table", action: () => { this.showTable(); } },
                { text: "3D heat map", action: () => { this.heatMap3D(); } },
            ]) }
        ]);
        this.page.setMenu(menu);

        this.colorMap = new ColorMap(0, 1);
        this.colorLegend = new ColorLegend(this.colorMap);
        this.colorLegend.setColorMapChangeEventListener(() => {
            this.refresh();
        });
        this.topLevel.appendChild(this.colorLegend.getHTMLRepresentation());
        let chartDiv = document.createElement("div");
        this.topLevel.appendChild(chartDiv);

        let canvasSize = Math.min(Resolution.getCanvasSize(this.getPage()).width, Resolution.getCanvasSize(this.getPage()).height);
        let chartSize = Math.min(Resolution.getChartSize(this.getPage()).width, Resolution.getChartSize(this.getPage()).height);
        this.heatMapCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "heatMap");
        this.heatMapChart = this.heatMapCanvas.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.controlPointsCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "controlPoints");
        this.controlPointsChart = this.controlPointsCanvas.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        page.setDataView(this);

        this.lampColNames = [
            TableView.uniqueColumnName(this.originalSchema, "LAMP1"),
            TableView.uniqueColumnName(this.originalSchema, "LAMP2")
        ];
    }

    public refresh() {
        let canvasSize = Math.min(Resolution.getCanvasSize(this.getPage()).width, Resolution.getCanvasSize(this.getPage()).height);
        let chartSize = Math.min(Resolution.getChartSize(this.getPage()).width, Resolution.getChartSize(this.getPage()).height);
        this.controlPointsCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize);
        this.controlPointsChart
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.heatMapCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize);
        this.heatMapChart
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize);
        this.updateControlPointsView();
        this.updateHeatMapView();
    }

    public updateRemoteTable(table: RemoteTableObject) {
        this.lampTableObject = table;
    }

    public updateControlPoints(pointSet: PointSet2D) {
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

    public updateHeatMap(heatMap: HeatMapData, lampTime: number) {
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
        this.colorMap.min = 1;
        this.colorMap.max = this.maxVal;
        this.colorMap.setLogScale(this.maxVal > ColorMap.logThreshold);

        this.updateHeatMapView();
    }
    public updateHeatMapView() {
        if (this.heatMapDots == null)
            return;
        let chartWidth = this.heatMapChart.attr("width");
        let chartHeight = this.heatMapChart.attr("height");

        this.heatMapChart.selectAll("*").remove();
        this.heatMapChart.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", chartWidth)
            .attr("height", chartHeight)
            .style("fill", "none")
            .style("stroke", "black");

        this.colorLegend.redraw();
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
            .style("fill", d => this.colorMap.apply(d.v));
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
            yBucketCount: yBuckets
        };
        let rr = this.originalTableObject.createLAMPMapRequest(this.controlPointsId, this.selectedColumns, this.controlPoints, this.lampColNames);
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
                    .on("drag", (p: Point2D, i: number, circles: Element[]) => {
                        let mouse = d3.mouse(plot.node());
                        mouse[0] = clamp(mouse[0], this.minX, this.minX + range);
                        mouse[1] = clamp(mouse[1], this.minY, this.minY + range);
                        p.x = mouse[0];
                        p.y = mouse[1];
                        d3.select(circles[i])
                            .attr("cx", clamp(mouse[0], this.minX, this.minX + range))
                            .attr("cy", clamp(mouse[1], this.minY, this.minY + range));
                    })
                    .on("end", (/*p: Point2D, i: number, circles: Element[]*/) => {
                        this.applyLAMP();
                    })
                )
    }
    private showTable() {
        let page = new FullPage("Table", this.page);
        this.getPage().insertAfterMe(page);
        let table = new TableView(this.lampTableObject.remoteObjectId, page);
        page.setDataView(table);
        let rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new TableRenderer(this.page, table, rr, false, new RecordOrder([])));
    }

    private heatMap3D() {
        let rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new SchemaCollector(this.getPage(), rr, this.lampTableObject, this.lampColNames));
    }
}

export class LAMPDialog extends Dialog {
    public static maxNumSamples = 100;

    constructor(private selectedColumns: string[], private page: FullPage,
                private schema: Schema, private remoteObject: TableView) {
        super("LAMP");
        this.addTextField("numSamples", "No. control points", "Integer", "20");
        this.addSelectField("controlPointSelection", "Control point selection", ["Random samples", "Category centroids"], "Random samples");
        let catColumns = [""];
        for (let i = 0; i < schema.length; i++)
            if (schema[i].kind == "Category")
                catColumns.push(schema[i].name);
        this.addSelectField("category", "Category for centroids", catColumns, "");
        this.addSelectField("controlPointProjection", "Control point projection", ["MDS"], "MDS");
        this.setAction(() => this.execute());
    }

    private execute() {
        let numSamples = this.getFieldValueAsInt("numSamples");
        if (numSamples > LAMPDialog.maxNumSamples) {
            this.page.reportError(`Too many samples. Use at most ${LAMPDialog.maxNumSamples}.`);
            return;
        }
        let selection = this.getFieldValue("controlPointSelection");
        let projection = this.getFieldValue("controlPointProjection");
        let category = this.getFieldValue("category");
        let rr: RpcRequest;
        switch (selection) {
            case "Random samples": {
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

        let newPage = new FullPage("LAMP", this.page);
        this.page.insertAfterMe(newPage);

        switch (projection) {
            case "MDS": {
                rr.invoke(new ControlPointsProjector(newPage, rr, this.remoteObject, this.selectedColumns, this.schema));
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
        super(page, operation, "Sampling control points");
    }

    onCompleted() {
        super.finished();
        if (this.remoteObject == null)
            return;
        let rr = this.tableObject.createMDSProjectionRequest(this.remoteObject.remoteObjectId);
        rr.invoke(new ControlPointsRenderer(this.page, rr, this.tableObject, this.schema, this.remoteObject.remoteObjectId, this.selectedColumns));
    }
}

class ControlPointsRenderer extends Renderer<PointSet2D> {
    private controlPointsView: ControlPointsView;
    private points: PointSet2D;

    constructor(page, operation, tableObject, schema, controlPointsId, private selectedColumns) {
        super(page, operation, "Projecting control points");
        this.controlPointsView = new ControlPointsView(tableObject, schema, page, controlPointsId, this.selectedColumns);
    }

    public onNext(result: PartialResult<PointSet2D>) {
        super.onNext(result);
        this.points = result.data;
        if (this.points != null)
            this.controlPointsView.updateControlPoints(this.points);
    }
}

class LAMPMapReceiver extends RemoteTableRenderer {
    constructor(page: FullPage, operation: ICancellable, private cpView: ControlPointsView,
                private arg: Histogram2DArgs) {
        super(page, operation, "Computing LAMP");
    }

    onCompleted() {
        super.finished();
        let lampTime = this.elapsedMilliseconds() / 1000;
        this.cpView.updateRemoteTable(this.remoteObject);
        let rr = this.remoteObject.createHeatMapRequest(this.arg);
        rr.invoke(new LAMPHeatMapReceiver(this.page, rr, this.cpView, lampTime));
    }
}

class LAMPHeatMapReceiver extends Renderer<HeatMapData> {
    private heatMap: HeatMapData;
    constructor(page: FullPage, operation: ICancellable,
                private controlPointsView: ControlPointsView, private lampTime: number) {
        super(page, operation, "Computing heat map")
    }

    onNext(result: PartialResult<HeatMapData>) {
        super.onNext(result);
        this.heatMap = result.data;
        if (this.heatMap != null) {
            this.controlPointsView.updateHeatMap(this.heatMap, this.lampTime);
        }
    }

}

class LAMPRangeCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    constructor(page: FullPage, operation: ICancellable, private cpView: ControlPointsView) {
        super(page, operation, "Getting LAMP ranges.")
    }

    onNext(result: PartialResult<Pair<BasicColStats, BasicColStats>>) {
        super.onNext(result);
        this.cpView.updateRanges(result.data.first, result.data.second);
    }
}

class SchemaCollector extends Renderer<NextKList> {
    private schema: Schema;
    constructor(page: FullPage, operation: ICancellable,
                private tableObject: RemoteTableObject, private lampColumnNames: string[]) {
        super(page, operation, "Getting new schema");
    }

    onNext(value: PartialResult<NextKList>) {
        this.schema = value.data.schema;
    }

    onCompleted() {
        super.onCompleted();
        let dialog = new HeatMapArrayDialog(this.lampColumnNames, this.page, this.schema, this.tableObject);
        dialog.show();
    }
}
