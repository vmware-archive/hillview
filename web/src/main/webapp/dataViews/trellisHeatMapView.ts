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
import {Renderer} from "../rpc";
import {Dialog} from "../ui/dialog";
import {TopMenu, SubMenu} from "../ui/menu";
import {Pair, truncate, significantDigits, ICancellable, PartialResult, Seed} from "../util";
import {AxisData} from "./axisData";
import {
    IColumnDescription, BasicColStats,
    ColumnAndRange, Schema, isNumeric, Histogram3DArgs, RecordOrder, RemoteObjectId
} from "../javaBridge";
import {CategoryCache} from "../categoryCache";
import {Point, Resolution, Size} from "../ui/ui";
import {IScrollTarget, ScrollBar} from "../ui/scroll";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {TextOverlay} from "../ui/textOverlay";
import {TableView, NextKReceiver} from "./tableView";
import {DistinctStrings} from "../distinctStrings";
import {RemoteTableObjectView, RemoteTableObject} from "../tableTarget";
import {PlottingSurface} from "../ui/plottingSurface";

export class HeatMapArrayData {
    buckets: number[][][];
    missingData: number;
    totalsize: number;
}

export interface HeatMapArrayArgs {
    cds: IColumnDescription[];
    uniqueStrings?: DistinctStrings;
    xStats?: BasicColStats;
    yStats?: BasicColStats;
}

class CompactHeatMapView {
    // We aim for this size. Square (apart from the label space), so it is
    // natural to tile. It is assumed that this will fit on the screen.
    public static readonly size: Size = {
        width: 200,
        height: 200 + Resolution.lineHeight
    };
    private static maxTextLabelLength = 10;
    private static axesTicks = 3;

    // Size of the entire drawing (label + chart)
    private size: Size;
    // Actual size of a rectangle in the chart.
    private dotSize: Size;

    private data: Map<number, number>; // 'sparse array' for fast querying of the values.

    // Information about the axes (range, ticks)
    private xAxisData;
    private yAxisData;
    // Elements
    private g: any; // g element with the drawing
    private chart: any; // chart on which the heatmap is drawn

    private axesG: any; // g element that will contain the axes
    private xAxis;
    private yAxis;
    private marker: any; // Marker that will indicate the x, y pair.
    // Lines that assist the marker.
    private xLine: any;
    private yLine: any;

    private pointDescription: TextOverlay;

    constructor(
        private parent: any, // Element where this heatmap is appended to.
        private pos: Point, // Position in parent
        private readonly chartSize: Size,
        private readonly labelSize: Size,
        private binLabel: string,
        public xDim: number,
        public yDim: number,
        private cds: IColumnDescription[],
        private xStats: BasicColStats,
        private yStats: BasicColStats
    ) {
        this.size = {
            width: Math.max(this.labelSize.width, this.chartSize.width),
            height: this.labelSize.height + this.chartSize.height
        };
        this.g = this.parent.append("g")
            .attr("transform", `translate(${pos.x}, ${pos.y})`);
        this.g.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.size.width)
            .attr("height", this.size.height)
            .style("fill-opacity", 0)
            .style("stroke", "black");

        binLabel = truncate(binLabel, CompactHeatMapView.maxTextLabelLength);
        this.g.append("text")
            .text(binLabel)
            .attr("text-anchor", "middle")
            .attr("x", this.size.width / 2)
            .attr("y", Resolution.lineHeight);

        this.chart = this.g.append("g")
            .attr("transform", `translate(0, ${Resolution.lineHeight})`)
            .attr("width", this.chartSize.width)
            .attr("height", this.chartSize.height);

        this.dotSize = {width: this.chartSize.width / this.xDim, height: this.chartSize.height / this.yDim};
        this.data = new Map<number, number>();

        this.xAxisData = new AxisData(cds[0], xStats, null, this.xDim);
        this.yAxisData = new AxisData(cds[1], yStats, null, this.yDim);
        this.xAxis = this.xAxisData.scaleAndAxis(this.chartSize.width, true, false).axis;
        this.yAxis = this.yAxisData.scaleAndAxis(this.chartSize.height, false, false).axis;
        this.xAxis.ticks(CompactHeatMapView.axesTicks);
        this.yAxis.ticks(CompactHeatMapView.axesTicks);
    }

    public put(x: number, y: number, val: number) {
        this.chart.append("rect")
            .attr("x", x * this.dotSize.width)
            .attr("y", this.chartSize.height - (y + 1) * this.dotSize.height)
            .attr("width", this.dotSize.width)
            .attr("height", this.dotSize.height)
            .attr("data-val", val);
        this.data.set(y * this.xDim + x, val);
    }

    // Returns the index of the cell where the given point is in. The
    // coordinates are relative to the origin of this chart.
    public getValAt(point: Point): number {
        let xIndex = Math.floor(point.x / this.dotSize.width);
        let yIndex = Math.floor((this.chart.attr("height") - point.y) / this.dotSize.height);
        let val = this.data.get(yIndex * this.xDim + xIndex);
        return val == null ? 0 : val;
    }

    public setColors(colorLegend: HeatmapLegendPlot) {
        this.chart.selectAll("rect")
            .datum(function() {return this.dataset;})
            .style("fill", (rect) => colorLegend.getColor(rect.val));
    }

    public getG() {
        return this.g;
    }

    private drawAxes() {
        this.hideAxes();
        this.axesG = this.parent.append("g")
            .attr("transform", `translate(${this.pos.x}, ${this.pos.y + Resolution.lineHeight})`);
        // Draw semi-tranparent rectangles s.t. the axes are readable.
        this.axesG.append("rect")
            .attr("x", -PlottingSurface.leftMargin)
            .attr("y", 0)
            .attr("width", PlottingSurface.leftMargin)
            .attr("height", this.chartSize.height)
            .attr("fill", "rgba(255, 255, 255, 0.9)");
        this.axesG.append("rect")
            .attr("x", 0)
            .attr("y", this.chartSize.height)
            .attr("width", this.chartSize.width)
            .attr("height", PlottingSurface.bottomMargin)
            .attr("fill", "rgba(255, 255, 255, 0.9)");

        // Draw the x and y axes
        this.axesG.append("g")
            .attr("class", "x-axis")
            .attr("transform", `translate(0, ${this.chartSize.height})`)
            .call(this.xAxis);
        this.axesG.append("g")
            .attr("class", "y-axis")
            .call(this.yAxis);

        // Draw a visual indicator with a circle and two lines.
        this.marker = this.axesG.append("circle")
            .attr("r", 4)
            .attr("cy", 0)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.xLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", this.chartSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");
        this.yLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", this.chartSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");

        this.pointDescription = new TextOverlay(this.axesG,
            [this.xAxisData.description.name, this.yAxisData.description.name, "value"],
            CompactHeatMapView.maxTextLabelLength);
    }

    // Returns the value (count in the histogram) under the mouse.
    public updateAxes(): void {
        let mouse = d3.mouse(this.chart.node());
        if (mouse[1] < 0) {
            this.hideAxes();
            return null;
        }
        if (this.axesG == null)
            this.drawAxes();

        let xVal = this.xAxisData.stats.min + (mouse[0] / this.chartSize.width) * (this.xAxisData.stats.max - this.xAxisData.stats.min);
        let yVal = this.yAxisData.stats.min + (1 - (mouse[1] / this.chartSize.height)) * (this.yAxisData.stats.max - this.yAxisData.stats.min);
        let val = this.getValAt({x: mouse[0], y: mouse[1]});

        // Set the visual markers
        this.marker.attr("cx", mouse[0])
            .attr("cy", mouse[1]);
        this.xLine.attr("x1", mouse[0])
            .attr("x2", mouse[0])
            .attr("y2", mouse[1]);
        this.yLine.attr("y1", mouse[1])
            .attr("y2", mouse[1])
            .attr("x2", mouse[0]);

        let x = mouse[0] + 5;
        let y = mouse[1] - 5;
        this.pointDescription.update([significantDigits(xVal), significantDigits(yVal), val.toString()], x, y);
    }

    public hideAxes() {
        if (this.axesG != null) {
            this.axesG.remove();
            this.axesG = null;
        }
    }
}

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatMapView extends RemoteTableObjectView implements IScrollTarget {
    // TODO: handle categorical values
    public args: HeatMapArrayArgs;
    private offset: number; // Offset from the start of the set of unique z-values.

    // UI elements
    private heatMaps: CompactHeatMapView[];
    private scrollBar: ScrollBar;
    private arrayAndScrollBar: HTMLDivElement;
    private colorLegend: HeatmapLegendPlot;
    private heatMapsSvg: any; // svg containing all heatmaps.
    private legendSurface: PlottingSurface;

    // Holds the state of which heatmap is hovered over.
    private mouseOverHeatMap: CompactHeatMapView;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId,
                page: FullPage, args: HeatMapArrayArgs,  private tableSchema: Schema) {
        super(remoteObjectId, originalTableId, page);
        this.args = args;
        this.offset = 0;
        if (this.args.cds.length != 3)
            throw "Expected 3 columns";

        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");

        let menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view."
                },{
                text: "swap axes",
                    action: () => { this.swapAxes(); },
                    help: "Swap the X and Y axes of all plots."
                }, { text: "table",
                    action: () => { this.showTable(); },
                    help: "Show the data underlying this view in a tabular view."
                },
            ]) }
        ]);
        this.page.setMenu(menu);
        this.legendSurface = new PlottingSurface(this.topLevel, page);
        //this.legendSurface.setMargins(0, 0, 0, 0);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);

        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
        // Add a listener that updates the heatmaps when the color map changes.
        this.colorLegend.setColorMapChangeEventListener(() => {
            this.reapplyColorMap();
        });

        // Div containing the array and the scrollbar
        this.arrayAndScrollBar = document.createElement("div");
        this.arrayAndScrollBar.classList.add("heatMapArray");
        this.topLevel.appendChild(this.arrayAndScrollBar);

        // Elements are added in this order so the scrollbars svg doesn't overlap
        // the value-indicator. They're drawn from right to left using reverse flex
        // in css.
        this.scrollBar = new ScrollBar(this);
        this.arrayAndScrollBar.appendChild(this.scrollBar.getHTMLRepresentation());

        let svgSize = this.actualDrawingSize();
        this.heatMapsSvg = d3.select(this.arrayAndScrollBar).append("svg")
            .attr("width", svgSize.width)
            .attr("height", svgSize.height);
    }

    // Returns the maximum size that the canvas is allowed to use
    private maxDrawingSize(): Size {
        let canvasSize = PlottingSurface.getDefaultCanvasSize(this.getPage());
        return {
            width: canvasSize.width - ScrollBar.barWidth,
            height: canvasSize.height
        }
    }

    // Returns the size that the canvas actually needs, since it contains
    // discrete elements.
    private actualDrawingSize(): Size {
        let [numCols, numRows] = this.numHeatMaps();
        return {
            width: numCols * CompactHeatMapView.size.width,
            height: numRows * (CompactHeatMapView.size.height + Resolution.lineHeight),
        }
    }

    // Returns the number of heatmaps that fit in the canvas
    private maxNumHeatMaps(): number {
        let [numRows, numCols] = this.numHeatMaps();
        return numCols * numRows;
    }

    // Returns the number of heatmaps in x and y directions.
    private numHeatMaps(): [number, number] {
        let size = this.maxDrawingSize();
        let numCols = Math.floor(size.width / CompactHeatMapView.size.width);
        let numRows = Math.floor(size.height / (CompactHeatMapView.size.height + Resolution.lineHeight));
        return [numCols, numRows];
    }

    public refresh(): void {
        this.initiateHeatMaps();
    }

    public swapAxes() {
        let xStats = this.args.xStats;
        this.args.xStats = this.args.yStats;
        this.args.yStats = xStats;
        let cdX = this.args.cds[0];
        this.args.cds[0] = this.args.cds[1];
        this.args.cds[1] = cdX;
        this.refresh();
    }

    public showTable() {
        let table = new TableView(this.remoteObjectId, this.originalTableId, this.page);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.args.cds[0],
            isAscending: true
        }, {
            columnDescription: this.args.cds[1],
            isAscending: true
        }, {
            columnDescription: this.args.cds[2],
            isAscending: true
        }]);
        let rr = table.createNextKRequest(order, null);
        let page = new FullPage("Table view", "Table", this.page);
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new NextKReceiver(page, table, rr, false, order));
    }

    public scrolledTo(position: number): void {
        this.offset = Math.min(
            Math.floor(position * this.args.uniqueStrings.size()),
            Math.max(0, this.args.uniqueStrings.size() - this.maxNumHeatMaps())
        );
        this.refresh();
    }

    public pageUp(): void {
        this.offset = Math.max(this.offset - this.maxNumHeatMaps(), 0);
        this.refresh();
    }

    public pageDown(): void {
        this.offset = Math.min(
            this.offset + this.maxNumHeatMaps(),
            this.args.uniqueStrings.size() - this.maxNumHeatMaps()
        );
        this.refresh();
    }

    public updateView(heatMapsArray: HeatMapArrayData, zBins: string[], timeInMs: number): void {
        if (heatMapsArray == null) {
            this.page.reportError("Did not receive data.");
            return;
        }

        this.colorLegend.clear();
        // Clear the heatmap svg, and resize it.
        this.heatMapsSvg.selectAll("*").remove();
        let svgSize = this.actualDrawingSize();
        this.heatMapsSvg
            .attr("width", svgSize.width)
            .attr("height", svgSize.height);

        let data = heatMapsArray.buckets;
        let xDim = data.length;
        let yDim = data[0].length;
        let zDim = data[0][0].length;

        let col = 0;
        let row = 0;
        let chartSize: Size = { // Constant, but should be specified by the caller.
            width: CompactHeatMapView.size.width,
            height: CompactHeatMapView.size.height
        };
        let labelSize: Size = {
            width: CompactHeatMapView.size.width,
            height: Resolution.lineHeight
        };
        let numCols = this.numHeatMaps()[0];

        let max = 0;
        this.heatMaps = new Array<CompactHeatMapView>(zDim);
        for (let z = 0; z < zDim; z++) {
            // Compute position in svg
            let pos = {x: col * CompactHeatMapView.size.width, y: row * (CompactHeatMapView.size.height + Resolution.lineHeight)};
            if (++col == numCols) {
                row++;
                col = 0;
            }

            let heatMap = new CompactHeatMapView(
                this.heatMapsSvg, pos, chartSize, labelSize,
                zBins[z], xDim, yDim, this.args.cds, this.args.xStats, this.args.yStats
            );
            for (let x = 0; x < xDim; x++) {
                for (let y = 0; y < yDim; y++) {
                    if (data[x][y][z] > 0) {
                        heatMap.put(x, y, data[x][y][z]);
                        max = Math.max(data[x][y][z], max);
                    }
                }
            }

            // Save it, as we need to set the colormap later.
            this.heatMaps[z] = heatMap;
        }
        // Update the color map based on the new values
        this.colorLegend.setData(1, max);
        this.reapplyColorMap();
        this.colorLegend.draw();

        this.scrollBar.setPosition(this.offset / this.args.uniqueStrings.size(),
            (this.offset + zBins.length) / this.args.uniqueStrings.size());

        // Register click listeners only after everything's set up.
        this.heatMapsSvg
            .on("mousemove", () => this.mouseMove())
            .on("mouseleave", () => this.mouseLeave());
        this.page.reportTime(timeInMs);
    }

    // Use the color map to set the colors in the heatmaps
    private reapplyColorMap() {
        this.heatMaps.forEach((heatMap) => heatMap.setColors(this.colorLegend));
    }

    private mouseMove() {
        // Calculate which heatmap is being moved over.
        let mouse = d3.mouse(this.heatMapsSvg.node());
        let [numCols, numRows] = this.numHeatMaps();
        let i = Math.floor(numCols * mouse[0] / this.heatMapsSvg.attr("width"));
        let j = Math.floor(numRows * mouse[1] / this.heatMapsSvg.attr("height"));
        let index = numCols * j + i;

        let newMouseOverHeatMap: CompactHeatMapView;
        if (index >= this.heatMaps.length)
            newMouseOverHeatMap = null;
        else
            newMouseOverHeatMap = this.heatMaps[index];

        // Hide the axes of the previously mouse-over'd heatmap
        if (newMouseOverHeatMap != this.mouseOverHeatMap && this.mouseOverHeatMap != null) {
            this.mouseOverHeatMap.hideAxes();
        }
        this.mouseOverHeatMap = newMouseOverHeatMap;

        // Show the new heatmap's axes
        if (this.mouseOverHeatMap != null)
            this.mouseOverHeatMap.updateAxes();
    }

    private mouseLeave() {
        // Hide the previously mouse-over'd heatmap
        if (this.mouseOverHeatMap != null) {
            this.mouseOverHeatMap.hideAxes();
            this.mouseOverHeatMap = null;
        }
    }

    public setStats(stats: Pair<BasicColStats, BasicColStats>): void {
        this.args.xStats = stats.first;
        this.args.yStats = stats.second;
    }

    public initiateHeatMaps(): void {
        // Number of actual bins is bounded by the number of distinct values.
        let numZBins = Math.min(this.maxNumHeatMaps(), this.args.uniqueStrings.size() - this.offset);
        let zBins = this.args.uniqueStrings.categoriesInRange(
            this.offset, this.offset + numZBins - 1, numZBins);
        let numXBuckets = CompactHeatMapView.size.width / Resolution.minDotSize;
        let numYBuckets = CompactHeatMapView.size.height / Resolution.minDotSize;

        let col0: ColumnAndRange = {
            columnName: this.args.cds[0].name,
            min: this.args.xStats.min,
            max: this.args.xStats.max,
            bucketBoundaries: null  // TODO
        };
        let col1: ColumnAndRange = {
            columnName: this.args.cds[1].name,
            min: this.args.yStats.min,
            max: this.args.yStats.max,
            bucketBoundaries: null  // TODO
        };
        let col2: ColumnAndRange = {
            columnName: this.args.cds[2].name,
            min: this.offset,
            max: this.offset + zBins.length - 1,
            bucketBoundaries: zBins  // TODO
        };
        let args: Histogram3DArgs = {
            first: col0,
            second: col1,
            third: col2,
            seed: Seed.instance.get(),
            samplingRate: 1.0, // TODO
            xBucketCount: numXBuckets,
            yBucketCount: numYBuckets,
            zBucketCount: zBins.length
        };

        let rr = this.createHeatMap3DRequest(args);
        rr.invoke(new HeatMap3DRenderer(this.getPage(), this, rr, zBins));
    }
}

/**
 * Receives the data range and initiates a new rendering.
 */
class Range2DRenderer extends Renderer<Pair<BasicColStats, BasicColStats>> {
    constructor(page: FullPage, protected view: TrellisHeatMapView, operation: ICancellable) {
        super(page, operation, "Get stats");
    }

    onNext(value: PartialResult<Pair<BasicColStats, BasicColStats>>) {
        super.onNext(value);
        this.view.setStats(value.data);
    }

    onCompleted(): void {
        super.onCompleted();
        this.view.initiateHeatMaps();
    }
}

/**
 * Receives data for the Trellis plot and updates the display.
 */
class HeatMap3DRenderer extends Renderer<HeatMapArrayData> {
    constructor(page: FullPage, protected view: TrellisHeatMapView, operation: ICancellable, private zBins: string[]) {
        super(page, operation, "3D heatmap render");
    }

    onNext(data: PartialResult<HeatMapArrayData>): void {
        super.onNext(data);
        this.view.updateView(data.data, this.zBins, this.elapsedMilliseconds());
    }
}

/**
 * Dialog to query user about parameters ot a Trellis plot of heatmaps.
 */
export class HeatMapArrayDialog extends Dialog {
    /**
     * Create a dialog to display a Trellis plot of heatmaps.
     * @param {string[]} selectedColumns  Columns selected by the user.
     * @param {FullPage} page             Page containing the original dataset.
     * @param {Schema} schema             Data schema.
     * @param {RemoteTableObject} remoteObject  Remote table object.
     * @param {boolean} fixedColumns      If true do not allow the user to change the
     *                                    first two selected columns.
     */
    constructor(private selectedColumns: string[], private page: FullPage,
                private schema: Schema, private remoteObject: RemoteTableObject,
                fixedColumns: boolean) {
        super("Heatmap array", "Draws a set of heatmap displays, one for each value in a categorical column.");
        let selectedNumColumns: string[] = [];
        let selectedCatColumn: string = "";
        let catColumns = [];
        let numColumns = [];
        for (let i = 0; i < schema.length; i++) {
            if (schema[i].kind == "Category")
                catColumns.push(schema[i].name);
            if (schema[i].kind == "Double" || schema[i].kind == "Integer")
                numColumns.push(schema[i].name)
        }
        selectedColumns.forEach((selectedColumn) => {
            if (catColumns.indexOf(selectedColumn) >= 0)
                selectedCatColumn = selectedColumn;
        });
        selectedColumns.forEach((selectedColumn) => {
            if (numColumns.indexOf(selectedColumn) >= 0)
                selectedNumColumns.push(selectedColumn);
        });
        if (selectedCatColumn == "" && catColumns.length > 0) {
            selectedCatColumn = catColumns[0];
        }
        this.addSelectField("col1", "Heatmap column 1: ",
            fixedColumns ? [selectedNumColumns[0]] : numColumns, selectedNumColumns[0],
            "Column to use for the X axis of the heatmap plot.");
        this.addSelectField("col2", "Heatmap column 2: ",
            fixedColumns ? [selectedNumColumns[1]] : numColumns, selectedNumColumns[1],
            "Column to use for the Y axis of the heatmap plot.");
        this.addSelectField("col3", "Array column: ", catColumns, selectedCatColumn,
            "Column used to group heapmap plots.  Must be a categorical column.");
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

        let newPage = new FullPage("Heatmaps by " + args.cds[2].name, "Trellis", this.page);
        this.page.insertAfterMe(newPage);

        let heatMapArrayView = new TrellisHeatMapView(
            this.remoteObject.remoteObjectId, this.remoteObject.originalTableId, newPage, args, this.schema);
        newPage.setDataView(heatMapArrayView);
        let cont = (operation: ICancellable) => {
            args.uniqueStrings = CategoryCache.instance.getDistinctStrings(
                this.remoteObject.originalTableId, categCol.name);
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
