import {RemoteObjectView, CallbackCollector} from "../rpc";
import {Dialog} from "../dialog";
import {RangeInfo, TableView, Schema, IColumnDescription, isNumeric, isCategorical, DistinctStrings} from "../table";
import {BasicColStats} from "../histogramBase";
import {FullPage, Size, Resolution, IHtmlElement, ScrollBar, IScrollTarget} from "../ui";
import {Pair, Triple} from "../util";
import {ColumnAndRange} from "../histogramBase";
import {HeatMapData} from "../heatmap";
import {HeatMapArrayArgs, HeatMapArrayData} from "./types";
import d3 = require('d3');

class ColorMap {
    public logScale: boolean;
    constructor(public max) {
        this.logScale = max > 20;
    }

    public setLogScale(logScale: boolean) {
        this.logScale = logScale;
    }

    public apply(x) {
        if (this.logScale)
            return this.applyLog(x);
        else
            return this.applyLinear(x);
    }

    private applyLinear(x) {
        return this.map(x / this.max);
    }

    private applyLog(x) {
        return this.map(Math.log(x) / Math.log(this.max));
    }

    private map(x: number) {
        return d3.interpolateWarm(x);
    }
}

class CompactHeatMapView implements IHtmlElement {
    // Size of a single heatmap in pixels
    public static readonly size: Size = {
        width: 200,
        height: 200
    }
    private static maxTextLabelLength = 10;

    private topLevel: HTMLElement;
    private chart: any;

    constructor(private binLabel: string) {
        this.topLevel = document.createElement("div")
        if (binLabel.length > CompactHeatMapView.maxTextLabelLength) {
            binLabel = binLabel.slice(0, CompactHeatMapView.maxTextLabelLength) + "...";
        }
        this.topLevel.appendChild(document.createElement("p")).textContent = binLabel;
        this.chart = d3.select(this.topLevel).append("svg")
            .attr("width", CompactHeatMapView.size.width)
            .attr("height", CompactHeatMapView.size.height)
    }

    public put(x, y, val) {
        this.chart.append("rect")
            .attr("x", x * Resolution.minDotSize)
            .attr("y", CompactHeatMapView.size.height - (y + 1) * Resolution.minDotSize)
            .attr("width", Resolution.minDotSize)
            .attr("height", Resolution.minDotSize)
            .attr("data-val", val);
    }

    public setColors(colorMap: ColorMap) {
        this.chart.selectAll("rect")
            .datum(function() {return this.dataset;})
            .attr("fill", (rect) => colorMap.apply(rect.val));
    }

    public getHTMLRepresentation() {
        return this.topLevel;
    }
}

export class HeatMapArrayView extends RemoteObjectView implements IScrollTarget {
    public args: HeatMapArrayArgs;
    private heatMaps: CompactHeatMapView[];
    private scrollBar: ScrollBar;
    private heatMapsDiv: HTMLDivElement;
    private offset: number;

    constructor(remoteObjectId: string, page: FullPage, args: HeatMapArrayArgs) {
        super(remoteObjectId, page);
        this.args = args;
        this.offset = 0;
        if (isNumeric(args.cds[0].kind) && isNumeric(args.cds[1].kind) && isCategorical(args.cds[2].kind))
            this.initiateNumNumCat();
        else if (isNumeric(args.cds[0].kind) && isNumeric(args.cds[1].kind) && isNumeric(args.cds[2].kind))
            this.initiateNumNumNum();

        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("heatMapArrayView");

        this.heatMapsDiv = document.createElement("div");
        this.heatMapsDiv.classList.add("heatMapArray");
        this.topLevel.appendChild(this.heatMapsDiv);

        this.scrollBar = new ScrollBar(this);
        this.topLevel.appendChild(this.scrollBar.getHTMLRepresentation());

        this.getPage().setDataView(this);
    }

    private maxNumHeatMaps() {
        // Compute the number of heat maps that fits on the page.
        let canvasSize = Resolution.getCanvasSize(this.getPage());
        // +2 to account for 1 pixel of border on each side.
        let numCols = Math.floor((canvasSize.width - ScrollBar.barWidth) / (CompactHeatMapView.size.width + 2));
        let numRows = Math.floor(canvasSize.height / (CompactHeatMapView.size.height + 2));
        return numCols * numRows;
    }

    public refresh() {

    }

    public scrolledTo(position: number): void {
        let oldOffset = this.offset;
        this.offset = Math.min(
            Math.floor(position * this.args.uniqueStrings.length),
            this.args.uniqueStrings.length - this.maxNumHeatMaps()
        )
        console.log(`Scroll: ${oldOffset} --> ${this.offset}`);
        this.updateZBins();
        this.initiateHeatMaps();
    }

    public pageUp(): void {
        let oldOffset = this.offset;
        console.log(`Page up: ofsset - numheatmaps: ${this.offset - this.maxNumHeatMaps()}`);
        this.offset = Math.max(
            this.offset - this.maxNumHeatMaps(),
            0
        );
        console.log(`Page up: ${oldOffset} --> ${this.offset}`);
        this.updateZBins();
        this.initiateHeatMaps();
    }

    public pageDown(): void {
        let oldOffset = this.offset;
        this.offset = Math.min(
            this.offset + this.maxNumHeatMaps(),
            this.args.uniqueStrings.length - this.maxNumHeatMaps()
        );
        console.log(`Page down: ${oldOffset} --> ${this.offset}`);
        this.updateZBins();
        this.initiateHeatMaps();
    }

    public updateView(heatMapsArray: HeatMapArrayData) {
        // Clean up before updating.
        if (this.heatMaps != null) {
            while (this.heatMapsDiv.hasChildNodes())
                this.heatMapsDiv.removeChild(this.heatMapsDiv.lastChild);
        }
        let data = heatMapsArray.buckets;
        let xPoints = data.length;
        let yPoints = data[0].length;
        let zPoints = data[0][0].length;

        let max = 0;
        this.heatMaps = new Array<CompactHeatMapView>(zPoints);
        for (let z = 0; z < zPoints; z++) {
            let heatMap = new CompactHeatMapView(this.args.zBins[z]);
            this.heatMaps.push(heatMap);
            this.heatMapsDiv.appendChild(heatMap.getHTMLRepresentation());
            for (let x = 0; x < xPoints; x++) {
                for (let y = 0; y < yPoints; y++) {
                    if (data[x][y][z] > 0) {
                        heatMap.put(x, y, data[x][y][z]);
                        max = Math.max(data[x][y][z], max);
                    }
                }
            }
        }
        let colorMap = new ColorMap(max);
        this.heatMaps.forEach((heatMap) => {
            heatMap.setColors(colorMap);
        });
        this.scrollBar.setPosition(this.offset / this.args.uniqueStrings.length, (this.offset + this.args.zBins.length) / this.args.uniqueStrings.length);
    }

    // Entry point. Start the appropriate sketches for the given arguments.
    public start(args: HeatMapArrayArgs) {
        this.args = args;
        if (isNumeric(args.cds[0].kind) && isNumeric(args.cds[1].kind) && isCategorical(args.cds[2].kind))
            this.initiateNumNumCat();
        else if (isNumeric(args.cds[0].kind) && isNumeric(args.cds[1].kind) && isNumeric(args.cds[2].kind))
            this.initiateNumNumNum();
    }

    private initiateNumNumCat() {
        this.initiateUniqueStrings();
    }

    private initiateUniqueStrings() {
        let rr = this.createRpcRequest("uniqueStrings", this.args.cds[2].name);
        rr.invoke(new CallbackCollector<DistinctStrings>(this.getPage(), rr, (distinctStrings) => {
            let uniqueStrings = distinctStrings.mySet;
            uniqueStrings.sort();
            this.args.uniqueStrings = uniqueStrings;
            this.initiateRange2D();
        }));
    }

    private initiateRange2D() {
        let rangeArgs: RangeInfo[] = [
            {columnName: this.args.cds[0].name},
            {columnName: this.args.cds[1].name}
        ];
        let rr = this.createRpcRequest("range2D", rangeArgs);
        rr.invoke(
            new CallbackCollector<Pair<BasicColStats, BasicColStats>>(this.getPage(), rr, (stats) => {
                this.setStats(stats);
                this.updateZBins();
                this.scrolledTo(0); // Triggers initiateHeatMaps.
            }, "Range 2D")
        );
    }

    private setStats(stats) {
        this.args.xStats = stats.first;
        this.args.yStats = stats.second;
    }

    // Use the offset to compute the new slice of z bins.
    private updateZBins() {

        // Number of actual bins is bounded by the number of distinct values.
        let numZBins = Math.min(this.maxNumHeatMaps(), this.args.uniqueStrings.length - this.offset);

        this.args.zBins = this.args.uniqueStrings.slice(this.offset, this.offset + numZBins);
    }

    private initiateHeatMaps() {
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
        }

        let rr = this.createRpcRequest("heatMap3D", heatMapArrayArgs)
        rr.invoke(
            new CallbackCollector<HeatMapArrayData>(this.getPage(), rr, (heatMapArray) => this.updateView(heatMapArray), "Heat Map Array")
        );
    }

    private initiateNumNumNum() {
        console.error("TODO: Implement");
    }
}
