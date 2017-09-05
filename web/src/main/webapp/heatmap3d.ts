import {
    FullPage, IHtmlElement, Point, Size, KeyCodes,
    significantDigits, formatNumber, translateString,
    IScrollTarget, ScrollBar
} from "./ui";
import d3 = require('d3');
import {RemoteObject, RemoteObjectView, Renderer, combineMenu, CombineOperators, SelectedObject, ZipReceiver} from "./rpc";
import {ColumnDescription, Schema, ContentsKind, TableView, RecordOrder, TableRenderer, RangeInfo} from "./table";
import {Pair, Triple, Converters, reorder, regression, ICancellable, PartialResult} from "./util";
import {
    BasicColStats, Histogram, ColumnAndRange, AnyScale, HistogramViewBase,
    FilterDescription
} from "./histogramBase";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {TopMenu, TopSubMenu} from "./menu";
import {Histogram2DRenderer, Make2DHistogram} from "./histogram2d";

// counterpart of Java class 'HeatMap3D'
export class HeatMap3DData {
    buckets: number[][][];
    missingData: number;
    totalsize: number;
}

class HeatMapLegend implements IHtmlElement {
    private outer: HTMLElement;

    // Fields for the axes.
    private static axesTicks = 3;
    private static readonly axesSize: Size = {
        width: 200,
        height: 200
    };
    private static readonly axesMargin = {
        top: 40, right: 20, bottom: 40, left: 55
    };

    private axesG: any;
    private xData: AxisData;
    private yData: AxisData;
    private xAxis: any;
    private yAxis: any;
    private xScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private yScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private marker: any;
    private xLine: any;
    private yLine: any;
    private xText: any;
    private yText: any;

    // Fields for color legend
    private static readonly colorLegendSize: Size = {
        width: 500,
        height: 20
    }
    private static readonly colorLegendMargin = {
        top: 0, right: 30, bottom: 45, left: 30
    }

    private colorLegendG: any;
    private max: number;
    private legendScale: any;
    private valIndicatorLine: any;
    private valIndicatorText: any;

    constructor() {
        this.outer = document.createElement("div");
        this.outer.classList.add("overlay");

        let axesSvg = d3.select(this.outer)
            .append("svg")
            .attr("class", "bottomright")
            .attr("width", HeatMapLegend.axesSize.width + HeatMapLegend.axesMargin.left + HeatMapLegend.axesMargin.right)
            .attr("height", HeatMapLegend.axesSize.height + HeatMapLegend.axesMargin.top + HeatMapLegend.axesMargin.bottom)
        this.axesG = axesSvg.append("g")
            .attr("transform", translateString(HeatMapLegend.axesMargin.left, HeatMapLegend.axesMargin.top))

        axesSvg.on("mouseover", () => {
            if (axesSvg.attr("class") == "bottomright")
                axesSvg.attr("class", "bottomleft");
            else
                axesSvg.attr("class", "bottomright");
        })

        let legendSvg = d3.select(this.outer)
            .append("svg")
            .attr("width", HeatMapLegend.colorLegendSize.width + HeatMapLegend.colorLegendMargin.left + HeatMapLegend.colorLegendMargin.right)
            .attr("height", HeatMapLegend.colorLegendSize.height + HeatMapLegend.colorLegendMargin.top + HeatMapLegend.colorLegendMargin.bottom)
            .attr("class", "bottommiddle");
        this.colorLegendG = legendSvg.append("g")
            .attr("transform", translateString(HeatMapLegend.colorLegendMargin.left, HeatMapLegend.colorLegendMargin.top))

        legendSvg.on("mouseover", () => {
            if (legendSvg.attr("class") == "bottommiddle")
                legendSvg.attr("class", "topmiddle");
            else
                legendSvg.attr("class", "bottommiddle");
        })
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }

    public setAxes(axes: AxisData[]) {
        this.xData = axes[0];
        this.yData = axes[1];
        [this.xAxis, this.xScale] = this.xData.getAxis(HeatMapLegend.axesSize.width, true);
        [this.yAxis, this.yScale] = this.yData.getAxis(HeatMapLegend.axesSize.height, false);
        this.xAxis.ticks(HeatMapLegend.axesTicks);
        this.yAxis.ticks(HeatMapLegend.axesTicks);
        this.drawAxes();
    }

    public setMax(max: number) {
        if (max < 1)
            this.max = 1;
        else
            this.max = max;
    }

    public drawAxes() {
        this.axesG.selectAll("*").remove();
        this.axesG.append("g")
            .attr("class", "x-axis")
            .attr("transform", translateString(0, HeatMapLegend.axesSize.height))
            .call(this.xAxis);
        this.axesG.append("g")
            .attr("class", "y-axis")
            .call(this.yAxis);
        this.axesG.append("text")
            .text(this.yData.description.name)
            .attr("transform-origin", "center top")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "hanging")
            .attr("transform", translateString(-HeatMapLegend.axesMargin.left + 5, 0.5 * HeatMapLegend.axesSize.height) + "rotate(-90)");
        this.axesG.append("text")
            .text(this.xData.description.name)
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "baseline")
            .attr("transform", translateString(0.5 * HeatMapLegend.axesSize.width, HeatMapLegend.axesSize.height + HeatMapLegend.axesMargin.bottom - 5));
        this.marker = this.axesG.append("circle")
            .attr("r", 5)
            .attr("cy", 0)
            .attr("cx", 0)
            .attr("fill", "blue")
        this.xLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", HeatMapLegend.axesSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");
        this.yLine = this.axesG.append("line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", HeatMapLegend.axesSize.height)
            .attr("y2", 0)
            .attr("stroke", "blue")
            .attr("stroke-dasharray", "5,5");
        this.xText = this.axesG.append("text")
            .attr("text-anchor", "left")
            .attr("alignment-baseline", "hanging")
            .attr("x", -HeatMapLegend.axesMargin.left + 5)
            .attr("y", -HeatMapLegend.axesMargin.top + 5);
        this.yText = this.axesG.append("text")
            .attr("text-anchor", "left")
            .attr("alignment-baseline", "hanging")
            .attr("x", -HeatMapLegend.axesMargin.left + 5)
            .attr("y", -HeatMapLegend.axesMargin.top + 20);
    }

    public drawColorLegend(): void {
        this.colorLegendG.selectAll("*").remove();
        let gradient = this.colorLegendG.append('defs')
            .append('linearGradient')
            .attr('id', 'gradient')
            .attr('x1', '0%')
            .attr('y1', '0%')
            .attr('x2', '100%')
            .attr('y2', '0%')
            .attr('spreadMethod', 'pad');

        for (let i = 0; i <= 100; i += 4) {
            gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", HeatMap3DView.colorMap(i / 100))
                .attr("stop-opacity", 1)
        }

        this.colorLegendG.append("rect")
            .attr("width", HeatMapLegend.colorLegendSize.width)
            .attr("height", HeatMapLegend.colorLegendSize.height)
            .style("fill", "url(#gradient)")
            .attr("x", 0)
            .attr("y", 0);

        // create a scale and axis for the legend
        this.legendScale = HeatMap3DView.scale(this.max, HeatMap3DView.logScale(this.max));

        let ticks = this.max > 10 ? 10 : this.max - 1;
        this.legendScale
            .domain([1, this.max])
            .range([0, HeatMapLegend.colorLegendSize.width])
            .ticks(ticks);

        let legendAxis = d3.axisBottom(this.legendScale);
        this.colorLegendG.append("g")
            .attr("transform", translateString(0, HeatMapLegend.colorLegendSize.height))
            .call(legendAxis);
        this.valIndicatorLine = this.colorLegendG.append("line")
            .attr("y1", 0)
            .attr("y2", HeatMapLegend.colorLegendSize.height)
            .attr("stroke", "white")
            .attr("visibility", "hidden")
        this.valIndicatorText = this.colorLegendG.append("text")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "baseline")
            .attr("x", HeatMapLegend.colorLegendSize.width / 2)
            .attr("y", HeatMapLegend.colorLegendSize.height + HeatMapLegend.colorLegendMargin.bottom - 5);
    }

    public update(mouseX, mouseY, val) {
        this.marker.attr("cx", mouseX)
            .attr("cy", mouseY);
        this.xLine.attr("x1", mouseX)
            .attr("x2", mouseX)
            .attr("y2", mouseY);
        this.yLine.attr("y1", mouseY)
            .attr("y2", mouseY)
            .attr("x2", mouseX);

        let x = HeatMapLegend.invert(mouseX, this.xScale, this.xData.description.kind, this.xData.buckets);
        let y = HeatMapLegend.invert(mouseY, this.yScale, this.yData.description.kind, this.yData.buckets);
        this.xText.text(this.xData.description.name + " = " + x);
        this.yText.text(this.yData.description.name + " = " + y);
        if (val != null && val >= 1) {
            let scaledVal = this.legendScale(val);
            this.valIndicatorLine
                .attr("x1", scaledVal)
                .attr("x2", scaledVal)
                .attr("visibility", "visible");
            this.valIndicatorText.text(val);
        } else {
            this.valIndicatorLine.attr("visibility", "hidden")
            this.valIndicatorText.text("0");
        }
    }

    static invert(v: number, scale: AnyScale, kind: ContentsKind, buckets: string[]): string {
        let inv = scale.invert(v);
        if (kind == "Integer")
            inv = Math.round(<number>inv);
        let result = String(inv);
        if (kind == "Category") {
            let index = Math.round(<number>inv);
            if (index >= 0 && index < buckets.length)
                result = buckets[index];
            else
                result = "";
        }
        else if (kind == "Integer" || kind == "Double")
            result = significantDigits(<number> inv);
        // For Date do nothing
        return result;
    }
}

export class AxisData {
    public constructor(public description: ColumnDescription,
                       public stats: BasicColStats,
                       public buckets: string[], // used only for categorical histograms
                       public subsampled?: boolean)
    {}

    public getAxis(length: number, bottom: boolean): [any, AnyScale] {
        // returns a pair scale/axis
        let scale: any = null;
        let resultScale: AnyScale = null;
        if (this.description.kind == "Double" ||
            this.description.kind == "Integer") {
            scale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
            resultScale = scale;
        } else if (this.description.kind == "Category") {
            let ticks: number[] = [];
            let labels: string[] = [];
            for (let i = 0; i < length; i++) {
                let index = i * (this.stats.max - this.stats.min) / length;
                index = Math.round(index);
                ticks.push(index * length / (this.stats.max - this.stats.min));
                labels.push(this.buckets[this.stats.min + index]);
            }

            scale = d3.scaleOrdinal()
                .domain(labels)
                .range(ticks);
            resultScale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
            // cast needed probably because the d3 typings are incorrect
        } else if (this.description.kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(this.stats.min);
            let maxDate: Date = Converters.dateFromDouble(this.stats.max);
            scale = d3
                .scaleTime()
                .domain([minDate, maxDate]);
            resultScale = scale;
        }
        if (bottom) {
            scale.range([0, length]);
            return [d3.axisBottom(scale), resultScale];
        } else {
            scale.range([length, 0]);
            return [d3.axisLeft(scale), resultScale];
        }
    }
}

export class HeatMap3DView extends RemoteObjectView implements IScrollTarget {
    protected topLevel: HTMLElement;
    public static readonly maxColumns = 4;
    public static readonly chartWidth = 200;  // pixels
    public static readonly chartHeight = 200;  // pixels
    public static readonly minDotSize = 3;  // pixels
    public static readonly margin = {
        top: 20,
        right: 0,
        bottom: 35,
        left: 35
    };
    protected page: FullPage;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private valueLabel: HTMLElement;
    protected chartsDiv: any;
    protected legendDiv: any;
    protected summary: any;
    protected pointWidth: number;
    protected pointHeight: number;
    private scrollBar: ScrollBar;

    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        zData: AxisData;
        missingData: number;
        data: number[][][];
        xPoints: number;
        yPoints: number;
        zPoints: number;
    };
    private overlayLegend: HeatMapLegend;
    private logScale: boolean;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");
        this.topLevel.classList.add("heatMapArray");
        this.logScale = false;
        let menu = new TopMenu( [
            { text: "View", subMenu: new TopSubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => { this.showTable(); } },
            ]) },
            {
                text: "Combine", subMenu: combineMenu(this)
            }
        ]);
        this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.topLevel.tabIndex = 1;
        let chartsAndScrollbarDiv = document.createElement("div");

        this.chartsDiv = d3.select(chartsAndScrollbarDiv)
            .append("div")
            .attr("class", "array");
        this.overlayLegend = new HeatMapLegend();
        this.topLevel.appendChild(this.overlayLegend.getHTMLRepresentation());

        this.scrollBar = new ScrollBar(this);

        chartsAndScrollbarDiv.style.flexDirection = "row";
        chartsAndScrollbarDiv.style.display = "flex";
        chartsAndScrollbarDiv.style.flexWrap = "nowrap";
        chartsAndScrollbarDiv.style.justifyContent = "flex-start";
        chartsAndScrollbarDiv.style.alignItems = "stretch";
        this.topLevel.appendChild(chartsAndScrollbarDiv);
        chartsAndScrollbarDiv.appendChild(this.scrollBar.getHTMLRepresentation());

    }

    // combine two views according to some operation
    combine(how: CombineOperators): void {
        let r = SelectedObject.current.getSelected();
        if (r == null) {
            this.page.reportError("No view selected");
            return;
        }

        let rr = this.createRpcRequest("zip", r.remoteObjectId);
        let renderer = (page: FullPage, operation: ICancellable) => {
            return new Make2DHistogram(
                page, operation,
                [this.currentData.xData.description, this.currentData.yData.description],
                this.tableSchema, true);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, renderer));
    }

    public static logScale(max: number) {
        return max > 20;
    }

    public static scale(max: number, logScale: boolean) {
        if (logScale) {
            let base = (max > 10000) ? 10 : 2;
            return d3.scaleLog().base(base);
        } else {
            return d3.scaleLinear();
        }
    }

    changeScale(): void {
        this.logScale = !this.logScale;
        this.refresh();
    }

    // show the table corresponding to the data in the heatmap
    showTable(): void {
        let table = new TableView(this.remoteObjectId, this.page);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true
        }]);
        let rr = table.createNextKRequest(order, null);
        let page = new FullPage();
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new TableRenderer(page, table, rr, false, order));
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    // Generates a string that encodes a call to the SVG translate method
    static translateString(x: number, y: number): string {
        return "translate(" + String(x) + ", " + String(y) + ")";
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.data,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.zData,
            this.currentData.missingData,
            0);
    }

    public pageDown() {
        console.log("TODO: Implement pageDown");
    }

    public pageUp() {
        console.log("TODO: Implement pageUp");
    }

    public scrolledTo() {
        console.log("TODO: Implement scrolledTo");
    }

    public updateView(data: number[][][], xData: AxisData, yData: AxisData, zData: AxisData,
                      missingData: number, elapsedMs: number) : void {
        this.page.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");
        if (data == null || data.length == 0) {
            this.page.reportError("No data to display");
            return;
        }
        let xPoints = data.length;
        let yPoints = data[0].length;
        let zPoints = data[0][0].length;
        if (yPoints == 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            data: data,
            xData: xData,
            yData: yData,
            zData: zData,
            missingData: missingData,
            xPoints: xPoints,
            yPoints: yPoints,
            zPoints: zPoints
        };


        let pageWidth = this.page.getWidthInPixels();
        let canvasWidth = HeatMap3DView.chartWidth;
        let canvasHeight = HeatMap3DView.chartHeight + HeatMap3DView.margin.top;
        this.pointWidth = HeatMap3DView.chartWidth / xPoints;
        this.pointHeight = HeatMap3DView.chartHeight / yPoints;
        interface Dot {
            x: number,
            y: number,
            v: number
        }
        let dots: Dot[][] = [];
        let max: number = 0;
        let visible: number = 0;
        let distinct: number = 0;
        for (let z = 0; z < zPoints; z++) {
            dots[z] = [];
            for (let x = 0; x < xPoints; x++) {
                for (let y = 0; y < yPoints; y++) {
                    let v = data[x][y][z];
                    if (v > max)
                        max = v;
                    if (v != 0) {
                        let rec = {
                            x: x * this.pointWidth,
                            y: HeatMap3DView.chartHeight - (y + 1) * this.pointHeight,  // +1 because it's the upper corner
                            v: v
                        };
                        visible += v;
                        distinct++;
                        dots[z].push(rec);
                    }
                }
            }
        }
        this.logScale = HeatMap3DView.logScale(max);

        this.overlayLegend.setAxes([xData, yData]);
        this.overlayLegend.setMax(max);
        this.overlayLegend.drawColorLegend();

        for (let z = 0; z < zPoints; z++) {
            let canvas = this.chartsDiv
                .append("svg")
                .attr("width", canvasWidth)
                .attr("height", canvasHeight)
                .attr("cursor", "crosshair");

            // The chart uses a fragment of the canvas offset by the margins
            let chart = canvas.append("g")
                .attr("transform", HeatMap3DView.translateString(0, HeatMap3DView.margin.top));
            canvas.on("mousemove", () => this.onMouseMove(chart, z));

            let chartTitle: string = "undefined";
            if (zData.description.kind == "Integer" || zData.description.kind == "Double") {
                let left = zData.stats.min + z * (zData.stats.max - zData.stats.min) / zPoints
                chartTitle = left.toFixed(2) + " ... ";
                if (z + 1 < zPoints)
                    chartTitle += (zData.stats.min + (z + 1) * (zData.stats.max - zData.stats.min) / zPoints).toFixed(2);
            } else if (zData.description.kind == "Category" || zData.description.kind == "String") {
                if (!zData.subsampled)
                    chartTitle = zData.buckets[z];
                else {
                    chartTitle = zData.buckets[z] + " ... ";
                    // The bin EXCLUDES the RHS, but right now there's no way of accessing the righternmost value in the bin...
                    if (z + 1 < zPoints)
                        chartTitle += zData.buckets[z + 1];
                    else
                        chartTitle += zData.stats.maxObject
                }
            }
            chart.append("text")
                .text(chartTitle)
                .attr("text-anchor", "middle")
                .attr("alignment-baseline", "baseline")
                .attr("transform", translateString(0.5 * canvasWidth, -5));

            chart.selectAll()
                .data(dots[z])
                .enter()
                .append("g")
                .append("svg:rect")
                .attr("class", "heatMapCell")
                .attr("x", d => d.x)
                .attr("y", d => d.y)
                .attr("width", this.pointWidth)
                .attr("height", this.pointHeight)
                .style("stroke-width", 0)
                .style("fill", d => this.color(d.v, max));
        }
    }

    static colorMap(d: number): string {
        return d3.interpolateWarm(d);
    }

    color(d: number, max: number): string {
        if (max == 1)
            return "black";
        if (d == 0)
            throw "Zero should not have a color";
        if (this.logScale)
            return HeatMap3DView.colorMap(Math.log(d) / Math.log(max));
        else
            return HeatMap3DView.colorMap((d - 1) / (max - 1));
    }

    onMouseMove(chart: any, z: number): void {
        let position = d3.mouse(chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let i = Math.floor(mouseX / this.pointWidth);
        let j = Math.floor((HeatMap3DView.chartHeight - mouseY) / this.pointHeight);
        let val = null;

        if (i >= 0 && i < this.currentData.xPoints && j >= 0 && j < this.currentData.yPoints) {
            val = this.currentData.data[i][j][z];
        }

        this.overlayLegend.update(mouseX, mouseY, val);
    }

    public static getRenderingSize(page: FullPage): Size {
        let width = page.getWidthInPixels();
        width = width - HeatMap3DView.margin.left - HeatMap3DView.margin.right;
        let height = HeatMap3DView.chartHeight - HeatMap3DView.margin.top - HeatMap3DView.margin.bottom;
        return { width: width, height: height };
    }
}

// Waits for all column stats to be received and then initiates a heatmap or 2Dhistogram.
export class Range3DCollector extends Renderer<Triple<BasicColStats, BasicColStats, BasicColStats>> {
    protected stats: Triple<BasicColStats, BasicColStats, BasicColStats>;
    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable,
                protected drawHeatMap: boolean,  // true - heatMap, false - histogram
                private zBuckets: string[],
                private subsampled: boolean = false
    ) {
        super(page, operation, "range3d");
    }

    public setValue(bcs: Triple<BasicColStats, BasicColStats, BasicColStats>): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: RemoteObject) {
        this.remoteObject = ro;
    }

    onNext(value: PartialResult<Triple<BasicColStats, BasicColStats, BasicColStats>>): void {
        super.onNext(value);
        this.setValue(value.data);
    }

    public draw(): void {
        let zBucketCount: number;
        if (this.zBuckets == null) {
            zBucketCount = 2 * HeatMap3DView.maxColumns;
        } else {
            zBucketCount = this.zBuckets.length;
        }
        let size = HeatMap3DView.getRenderingSize(this.page);
        let xBucketCount = Math.floor(HeatMap3DView.chartWidth / (HeatMap3DView.minDotSize));
        let yBucketCount = Math.floor(HeatMap3DView.chartWidth / (HeatMap3DView.minDotSize));
        let arg0: ColumnAndRange = {
            columnName: this.cds[0].name,
            min: this.stats.first.min,
            max: this.stats.first.max,
            samplingRate: 1.0,
            bucketCount: xBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: null // TODO
        };
        let arg1: ColumnAndRange = {
            columnName: this.cds[1].name,
            min: this.stats.second.min,
            max: this.stats.second.max,
            samplingRate: 1.0,
            bucketCount: yBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: null // TODO
        };
        let arg2: ColumnAndRange = {
            columnName: this.cds[2].name,
            min: this.stats.third.min,
            max: this.stats.third.max,
            samplingRate: 1.0,
            bucketCount: zBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: this.zBuckets
        };
        let args = {
            first: arg0,
            second: arg1,
            third: arg2
        };

        let rr = this.remoteObject.createRpcRequest("heatMap3D", args);
        if (this.operation != null)
            rr.setStartTime(this.operation.startTime());
        let renderer: Renderer<HeatMap3DData> = new HeatMap3DRenderer(this.page,
            this.remoteObject.remoteObjectId, this.tableSchema,
            this.cds, [this.stats.first, this.stats.second, this.stats.third], args, this.subsampled, rr);
        rr.invoke(renderer);
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
        // probably some error occurred
            return;
        this.draw();
    }
}

class DistinctStrings {
    mySet: string[];
    truncated: boolean;
    rowCount: number;
    missingCount: number;
}

// First step of a heatmap for a categorical column:
// create a numbering for the strings
export class NumberStringsHeatMap extends Renderer<DistinctStrings> {
    protected contentsInfo: DistinctStrings;

    public constructor(protected cd: ColumnDescription, page: FullPage, operation: ICancellable,
                       private callback: (bcs: BasicColStats, vals: string[]) => void) {
        super(page, operation, "Create converter");
        this.contentsInfo = null;
    }

    public onNext(value: PartialResult<DistinctStrings>): void {
        super.onNext(value);
        this.contentsInfo = value.data;
    }

    public onCompleted(): void {
        if (this.contentsInfo == null)
            return;
        super.finished();
        let strings = this.contentsInfo.mySet;
        if (strings.length == 0) {
            this.page.reportError("No data in column");
            return;
        }
        strings.sort();

        let bcs: BasicColStats = {
            momentCount: 0,
            min: 0,
            max: strings.length - 1,
            minObject: strings[0],
            maxObject: strings[strings.length - 1],
            moments: [],
            presentCount: this.contentsInfo.rowCount - this.contentsInfo.missingCount,
            missingCount: this.contentsInfo.missingCount
        };
        this.callback(bcs, strings);
    }
}

// Renders a heatmap
export class HeatMap3DRenderer extends Renderer<HeatMap3DData> {
    protected heatMap: HeatMap3DView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                private colAndRanges: Triple<ColumnAndRange, ColumnAndRange, ColumnAndRange>,
                private subsampled: boolean,
                operation: ICancellable) {
        super(new FullPage(), operation, "heatmap3d");
        page.insertAfterMe(this.page);
        this.heatMap = new HeatMap3DView(remoteTableId, schema, this.page);
        this.page.setDataView(this.heatMap);
        if (cds.length != 3)
            throw "Expected 3 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMap3DData>): void {
        super.onNext(value);
        if (value == null)
            return;
        let xAxisData = new AxisData(this.cds[0], this.stats[0], null);
        let yAxisData = new AxisData(this.cds[1], this.stats[1], null);
        let zAxisData = new AxisData(this.cds[2], this.stats[2], this.colAndRanges.third.bucketBoundaries, this.subsampled);
        this.heatMap.updateView(value.data.buckets, xAxisData, yAxisData, zAxisData,
            value.data.missingData, this.elapsedMilliseconds());
    }
}
