/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {Renderer} from "./rpc";
import {Dialog} from "./dialog";
import {TableView} from "./table";
import {
    FullPage, Size, Resolution, IHtmlElement, ScrollBar, significantDigits, IScrollTarget,
    removeAllChildren
} from "./ui";
import {Pair, Triple, truncate, Point2D, ICancellable, PartialResult} from "./util";
import {ColorMap} from "./vis";
import d3 = require('d3');
import {AxisData} from "./heatMap";
import {
    RemoteTableObjectView, IColumnDescription, BasicColStats, DistinctStrings,
    ColumnAndRange, Schema, isNumeric, RemoteTableObject
} from "./tableData";
import {CategoryCache} from "./categoryCache";

export class HeatMapArrayData {
    buckets: number[][][];
    missingData: number;
    totalsize: number;
}

export interface HeatMapArrayArgs {
    cds: IColumnDescription[];
    uniqueStrings?: DistinctStrings;
    zBins?: string[];
    subsampled?: boolean;
    xStats?: BasicColStats;
    yStats?: BasicColStats;
}

export class LegendOverlay implements IHtmlElement {
    private outer: HTMLElement;

    // Fields for the axes.
    private xAxisData: AxisData;
    private yAxisData: AxisData;

    // -- UI variables
    private static axesTicks = 3;
    private static colorLegendHeight = 50;
    private axesSize: Size;

    // -- Elements
    private axesG: any; // g element that will contain the axes
    private xAxis: any;
    private yAxis: any;
    private marker: any; // Marker that will indicate the x, y pair.
    // Lines that assist the marker.
    private xLine: any;
    private yLine: any;
    // Text that show the values as numbers on the screen.
    private xText: any;
    private yText: any;

    // Fields for color legend
    private colorMap: ColorMap;
    // g element that will contain the legend.
    private legendG: any;

    constructor(cds: IColumnDescription[],
                xStats: BasicColStats, yStats: BasicColStats,
                xStrings: DistinctStrings, yStrings: DistinctStrings,
                xBuckets: number, yBuckets: number) {
        this.outer = document.createElement("div");
        this.outer.classList.add("overlay");

        this.axesSize = {
            width: CompactHeatMapView.size.width,
            height: CompactHeatMapView.size.height
        };

        this.xAxisData = new AxisData(null, cds[0], xStats, xStrings, xBuckets);
        this.yAxisData = new AxisData(null, cds[1], yStats, yStrings, yBuckets);
        this.xAxis = this.xAxisData.scaleAndAxis(this.axesSize.width, true).axis;
        this.yAxis = this.yAxisData.scaleAndAxis(this.axesSize.height, false).axis;
        this.xAxis.ticks(LegendOverlay.axesTicks);
        this.yAxis.ticks(LegendOverlay.axesTicks);

        let axesSvg = d3.select(this.outer)
            .append("svg")
            .attr("class", "bottomright")
            .attr("width", this.axesSize.width + Resolution.leftMargin + Resolution.rightMargin)
            .attr("height", this.axesSize.height + Resolution.topMargin + Resolution.bottomMargin);
        this.axesG = axesSvg.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`);

        let legendSvg = d3.select(this.outer).append("svg")
            .attr("class", "bottommiddle")
            .attr("width", Resolution.legendWidth + Resolution.leftMargin + Resolution.rightMargin)
            .attr("height", 50);
        this.legendG = legendSvg.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, 0)`)
            .attr("width", Resolution.legendWidth)
            .attr("height", LegendOverlay.colorLegendHeight);

        // Make the overlays move over on hover.
        axesSvg.on("mouseover", () => {
            if (axesSvg.attr("class") == "bottomright")
                axesSvg.attr("class", "bottomleft");
            else
                axesSvg.attr("class", "bottomright");
        });
        legendSvg.on("mouseover", () => {
            if (legendSvg.attr("class") == "bottommiddle")
            legendSvg.attr("class", "topmiddle");
            else
            legendSvg.attr("class", "bottommiddle");
        });
    }

    public draw() {
        this.drawAxes();
        this.drawColorLegend();
    }

    public remove() {
        this.outer.remove();
    }

    public setColorMap(colorMap: ColorMap){
        this.colorMap = colorMap;
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    public drawAxes() {
        this.axesG.selectAll("*").remove();
        this.axesG.append("g")
            .attr("class", "x-axis")
            .attr("transform", `translate(0, ${this.axesSize.height})`)
            .call(this.xAxis);
        this.axesG.append("g")
            .attr("class", "y-axis")
            .call(this.yAxis);
        this.axesG.append("text")
            .text(this.yAxisData.description.name)
            .attr("transform-origin", "center top")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "hanging")
            .attr("transform", `translate(${-Resolution.leftMargin}, ${0.5 * this.axesSize.height})rotate(-90)`);
        this.axesG.append("text")
            .text(this.xAxisData.description.name)
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "baseline")
            .attr("transform", `translate(${0.5 * this.axesSize.width}, ${this.axesSize.height + Resolution.bottomMargin - 5})`);
        this.marker = this.axesG.append("circle")
            .attr("r", 4)
            .attr("cy", 0)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.xLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", this.axesSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");
        this.yLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", this.axesSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");
        this.xText = this.axesG.append("text")
            .attr("text-anchor", "left")
            .attr("alignment-baseline", "hanging")
            .attr("x", -Resolution.leftMargin + 5)
            .attr("y", -Resolution.topMargin + 5);
        this.yText = this.axesG.append("text")
            .attr("text-anchor", "left")
            .attr("alignment-baseline", "hanging")
            .attr("x", -Resolution.leftMargin + 5)
            .attr("y", -Resolution.topMargin + 25);
    }

    public drawColorLegend(): void {
        this.legendG.selectAll("*").remove();
        this.colorMap.drawLegend(this.legendG);
    }

    public update(pos: Point2D) {
        let xVal = this.xAxisData.stats.min + pos.x * (this.xAxisData.stats.max - this.xAxisData.stats.min);
        let yVal = this.yAxisData.stats.min + (1 - pos.y) * (this.yAxisData.stats.max - this.yAxisData.stats.min);

        // Transform to the coordinate system of the legend axis.
        pos.x *= this.axesSize.width;
        pos.y *= this.axesSize.height;

        this.marker.attr("cx", pos.x)
            .attr("cy", pos.y);
        this.xLine.attr("x1", pos.x)
            .attr("x2", pos.x)
            .attr("y2", pos.y);
        this.yLine.attr("y1", pos.y)
            .attr("y2", pos.y)
            .attr("x2", pos.x);

        this.xText.text(this.xAxisData.description.name + " = " + significantDigits(xVal));
        this.yText.text(this.yAxisData.description.name + " = " + significantDigits(yVal));
    }
}

export class CompactHeatMapView implements IHtmlElement {
    // We aim for this size. Square, so it natural to tile.
    // It is assumed that this will fit on the screen.
    public static readonly size: Size = {
        width: 200,
        height: 200
    };
    private static maxTextLabelLength = 10;
    // The actual size of the canvas.
    public size: Size;

    private topLevel: HTMLElement;
    private chart: any; // chart on which the heat map is drawn
    // Actual size of a rectangle on the canvas.
    private dotSize: Size;
    private data: Map<number, number>; // 'sparse array' for fast querying of the values.

    constructor(private binLabel: string, public xDim: number, public yDim: number) {
        this.topLevel = document.createElement("div");

        binLabel = truncate(binLabel, CompactHeatMapView.maxTextLabelLength);
        this.topLevel.appendChild(document.createElement("p")).textContent = binLabel;

        this.size = {
            width: CompactHeatMapView.size.width,
            height: CompactHeatMapView.size.height
        };

        this.chart = d3.select(this.topLevel).append("svg")
            .attr("width", this.size.width)
            .attr("height", this.size.height);

        this.dotSize = {width: this.size.width / this.xDim, height: this.size.height / this.yDim};
        this.data = new Map<number, number>();
    }

    public put(x, y, val) {
        this.chart.append("rect")
            .attr("x", x * this.dotSize.width)
            .attr("y", CompactHeatMapView.size.height - (y + 1) * this.dotSize.height)
            .attr("width", this.dotSize.width)
            .attr("height", this.dotSize.height)
            .attr("data-val", val);
        this.data.set(y * this.xDim + x, val);
    }

    // Returns the index of the cell where the given point is in.
    public getValAt(mouse: Point2D): number {
        let xIndex = Math.floor(mouse.x / this.dotSize.width);
        let yIndex = Math.floor((this.chart.attr("height") - mouse.y) / this.dotSize.height);
        let val = this.data.get(yIndex * this.xDim + xIndex);
        return val == null ? 0 : val;
    }

    public setColors(colorMap: ColorMap) {
        this.chart.selectAll("rect")
            .datum(function() {return this.dataset;})
            .attr("fill", (rect) => colorMap.apply(rect.val));
    }

    public setListener(legend: LegendOverlay) {
        this.chart.on("mousemove", () => {
            let mousePos = d3.mouse(this.chart.node());
            let mouse: Point2D = {x: mousePos[0], y: mousePos[1]};

            // Compute position in [0, 1] x [0, 1] range.
            // Legend will compute the actual values based on the range.
            let pos = {
                x: mouse.x / (this.dotSize.width * this.xDim),
                y: mouse.y / (this.dotSize.width * this.xDim)
            };

            legend.update(pos);
        });
    }

    public getHTMLRepresentation() {
        return this.topLevel;
    }
}

export class HeatMapArrayView extends RemoteTableObjectView implements IScrollTarget {
    // TODO: handle categorical values
    public args: HeatMapArrayArgs;
    private heatMaps: CompactHeatMapView[];
    private scrollBar: ScrollBar;
    private legendOverlay: LegendOverlay;
    private heatMapsDiv: HTMLDivElement;
    private offset: number; // Offset from the start of the set of unique z-values.

    constructor(remoteObjectId: string, page: FullPage, args: HeatMapArrayArgs) {
        super(remoteObjectId, page);
        this.args = args;
        this.offset = 0;
        if (this.args.cds.length != 3)
            throw "Expected 3 columns";

        this.heatMapsDiv = document.createElement("div");
        this.heatMapsDiv.classList.add("heatMapArray");

        this.scrollBar = new ScrollBar(this);

        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("heatMapArrayView");
        this.topLevel.appendChild(this.heatMapsDiv);
        this.topLevel.appendChild(this.scrollBar.getHTMLRepresentation());
    }

    public maxNumHeatMaps(): number {
        // Compute the number of heat maps that fits on the page.
        let canvasSize = Resolution.getCanvasSize(this.getPage());
        // +2 to account for 1 pixel of border on each side.
        let numCols = Math.floor((canvasSize.width - ScrollBar.barWidth) / (CompactHeatMapView.size.width + 2));
        let numRows = Math.floor(canvasSize.height / (CompactHeatMapView.size.height + 2));
        return numCols * numRows;
    }

    public refresh(): void {
        this.initiateHeatMaps();
    }

    public scrolledTo(position: number): void {
        this.offset = Math.min(
            Math.floor(position * this.args.uniqueStrings.size()),
            Math.max(0, this.args.uniqueStrings.size() - this.maxNumHeatMaps())
        );
        this.refresh();
    }

    public pageUp(): void {
        this.offset = Math.max(
            this.offset - this.maxNumHeatMaps(),
            0
        );
        this.refresh();
    }

    public pageDown(): void {
        this.offset = Math.min(
            this.offset + this.maxNumHeatMaps(),
            this.args.uniqueStrings.size() - this.maxNumHeatMaps()
        );
        this.refresh();
    }

    public updateView(heatMapsArray: HeatMapArrayData): void {
        if (this.heatMaps != null)
            removeAllChildren(this.heatMapsDiv);
        if (heatMapsArray == null) {
            this.page.reportError("Did not receive data.");
            return;
        }

        let data = heatMapsArray.buckets;
        let xDim = data.length;
        let yDim = data[0].length;
        let zDim = data[0][0].length;

        let max = 0;
        this.heatMaps = new Array<CompactHeatMapView>(zDim);
        for (let z = 0; z < zDim; z++) {
            let heatMap = new CompactHeatMapView(this.args.zBins[z], xDim, yDim);
            this.heatMaps.push(heatMap);
            this.heatMapsDiv.appendChild(heatMap.getHTMLRepresentation());
            for (let x = 0; x < xDim; x++) {
                for (let y = 0; y < yDim; y++) {
                    if (data[x][y][z] > 0) {
                        heatMap.put(x, y, data[x][y][z]);
                        max = Math.max(data[x][y][z], max);
                    }
                }
            }
        }
        let colorMap = new ColorMap(max);
        this.legendOverlay.setColorMap(colorMap);
        this.legendOverlay.draw();
        this.heatMaps.forEach((heatMap) => {
            heatMap.setColors(colorMap);
            heatMap.setListener(this.legendOverlay);
        });

        this.scrollBar.setPosition(this.offset / this.args.uniqueStrings.size(),
            (this.offset + this.args.zBins.length) / this.args.uniqueStrings.size());
    }

   public viewComplete(): void {
       // TODO: properly compute number of buckets
       let numXBuckets = CompactHeatMapView.size.width / Resolution.minDotSize;
       let numYBuckets = CompactHeatMapView.size.height / Resolution.minDotSize;
       this.legendOverlay = new LegendOverlay(
            this.args.cds, this.args.xStats, this.args.yStats, null, null, numXBuckets, numYBuckets);
        this.getHTMLRepresentation().appendChild(this.legendOverlay.getHTMLRepresentation());
    }

    public setStats(stats: Pair<BasicColStats, BasicColStats>): void {
        this.args.xStats = stats.first;
        this.args.yStats = stats.second;
    }

    public initiateHeatMaps(): void {
        // Number of actual bins is bounded by the number of distinct values.
        let numZBins = Math.min(this.maxNumHeatMaps(), this.args.uniqueStrings.size() - this.offset);
        this.args.zBins = this.args.uniqueStrings.categoriesInRange(
            this.offset, this.offset + numZBins - 1, numZBins);

        let numXBuckets = CompactHeatMapView.size.width / Resolution.minDotSize;
        let numYBuckets = CompactHeatMapView.size.height / Resolution.minDotSize;

        let heatMapArrayArgs: Triple<ColumnAndRange, ColumnAndRange, ColumnAndRange> = {
            first: {
                columnName: this.args.cds[0].name,
                min: this.args.xStats.min,
                max: this.args.xStats.max,
                cdfBucketCount: 0,
                bucketCount: numXBuckets,
                samplingRate: 1,
                bucketBoundaries: null
            },
            second: {
                columnName: this.args.cds[1].name,
                min: this.args.yStats.min,
                max: this.args.yStats.max,
                cdfBucketCount: 0,
                bucketCount: numYBuckets,
                samplingRate: 1,
                bucketBoundaries: null
            },
            third: {
                columnName: this.args.cds[2].name,
                min: this.offset,
                max: this.offset + this.args.zBins.length - 1,
                cdfBucketCount: 0,
                bucketCount: this.args.zBins.length,
                samplingRate: 1,
                bucketBoundaries: this.args.zBins
            }
        };

        let rr = this.createHeatMap3DRequest(heatMapArrayArgs);
        rr.invoke(new HeatMap3DRenderer(this.getPage(), this, rr));
    }
}

class Range2DRenderer extends Renderer<Pair<BasicColStats, BasicColStats>> {
    constructor(page: FullPage, protected view: HeatMapArrayView, operation: ICancellable) {
        super(page, operation, "Get stats");
    }

    onNext(value: PartialResult<Pair<BasicColStats, BasicColStats>>) {
        super.onNext(value);
        this.view.setStats(value.data);
    }

    onCompleted(): void {
        super.onCompleted();
        this.view.initiateHeatMaps();
        this.view.viewComplete();
    }
}

class HeatMap3DRenderer extends Renderer<HeatMapArrayData> {
    constructor(page: FullPage, protected view: HeatMapArrayView, operation: ICancellable) {
        super(page, operation, "3D Heat map render");
    }

    onNext(data: PartialResult<HeatMapArrayData>): void {
        super.onNext(data);
        this.view.updateView(data.data);
    }
}

export class HeatMapArrayDialog extends Dialog {
    constructor(private selectedColumns: string[], private page: FullPage,
                private schema: Schema, private remoteObject: RemoteTableObject) {
        super("Heat map array");
        this.addSelectField("col1", "Heat map column 1: ", selectedColumns, selectedColumns[0]);
        this.addSelectField("col2", "Heat map column 2: ", selectedColumns, selectedColumns[1]);
        this.addSelectField("col3", "Array column: ", selectedColumns, selectedColumns[2]);
        this.setAction(() => this.execute());
    }

    private execute(): void {
        let args = this.parseFields();
        if (!isNumeric(args.cds[0].kind) || !isNumeric(args.cds[1].kind)) {
            this.page.reportError("First and second colum must be numeric");
            return;
        }

        let categCol = args.cds[2];
        if (categCol.kind != "Category") {
            this.page.reportError("Last column must be categorical");
            return;
        }

        let newPage = new FullPage();
        this.page.insertAfterMe(newPage);

        let heatMapArrayView = new HeatMapArrayView(this.remoteObject.remoteObjectId, newPage, args);
        newPage.setDataView(heatMapArrayView);
        let cont = (operation: ICancellable) => {
            args.uniqueStrings = CategoryCache.instance.getDistinctStrings(categCol.name);
            let rr = heatMapArrayView.createRange2DColsRequest(args.cds[0].name, args.cds[1].name);
            rr.chain(operation);
            rr.invoke(new Range2DRenderer(newPage, heatMapArrayView, rr));
        };
        CategoryCache.instance.retrieveCategoryValues(this.remoteObject, [categCol.name], this.page, cont);
    }

    private parseFields(): HeatMapArrayArgs {
        let cd1 = TableView.findColumn(this.schema, this.getFieldValue("col1"));
        let cd2 = TableView.findColumn(this.schema, this.getFieldValue("col2"));
        let cd3 = TableView.findColumn(this.schema, this.getFieldValue("col3"));
        return {
            cds: [cd1, cd2, cd3],
        };
    }
}
