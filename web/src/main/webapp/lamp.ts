import {Dialog} from "./dialog";
import {TopMenu, TopSubMenu} from "./menu";
import {TableView, TableRenderer} from "./table";
import {ContentsKind, RangeInfo, BasicColStats, Schema, RemoteTableObject, RemoteTableObjectView, RemoteTableRenderer, ColumnAndRange, RecordOrder} from "./tableData";
import {FullPage, IDataView, Resolution} from "./ui";
import {Renderer, RpcReceiver, RpcRequest} from "./rpc";
import {PartialResult, Point2D, clamp, Pair} from "./util";
import {HeatMapData} from "./heatMap";
import {ColorMap} from "./vis";
import d3 = require('d3');

export class PointSet2D {
    points: Point2D[];
}

class ControlPointsView extends RemoteTableObjectView {
    private minX: number;
    private minY: number;
    private maxX: number;
    private maxY: number;
    private heatMapCanvas: any;
    private heatMapChart: any;
    private controlPointsCanvas: any;
    private controlPointsChart: any;
    public controlPoints: PointSet2D;
    private heatMap: HeatMapData;
    private lampTableObject: RemoteTableObject;

    constructor(private originalTableObject: RemoteTableObject, page: FullPage, private controlPointsId, private selectedColumns) {
        super(originalTableObject.remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");
        this.topLevel.classList.add("lampView");

        let menu = new TopMenu( [
            { text: "View", subMenu: new TopSubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "update ranges", action: () => { this.fetchNewRanges(); } },
                { text: "table", action: () => { this.showTable(); } },
            ]) }
        ]);

        this.topLevel.appendChild(menu.getHTMLRepresentation());

        let chartDiv = document.createElement("div")
        this.topLevel.appendChild(chartDiv);

        let canvasSize = Math.min(Resolution.getCanvasSize(this.getPage()).width, Resolution.getCanvasSize(this.getPage()).height);
        let chartSize = Math.min(Resolution.getChartSize(this.getPage()).width, Resolution.getChartSize(this.getPage()).height)
        this.heatMapCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "heatMap")
        this.heatMapChart = this.heatMapCanvas.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize)
        this.controlPointsCanvas = d3.select(chartDiv).append("svg")
            .attr("width", canvasSize)
            .attr("height", canvasSize)
            .attr("class", "controlPoints")
        this.controlPointsChart = this.controlPointsCanvas.append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize)
        page.setDataView(this);
    }

    public refresh() {
        let canvasSize = Math.min(Resolution.getCanvasSize(this.getPage()).width, Resolution.getCanvasSize(this.getPage()).height);
        let chartSize = Math.min(Resolution.getChartSize(this.getPage()).width, Resolution.getChartSize(this.getPage()).height)
        this.controlPointsCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize)
        this.controlPointsChart
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize)
        this.heatMapCanvas
            .attr("width", canvasSize)
            .attr("height", canvasSize)
        this.heatMapChart
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .attr("width", chartSize)
            .attr("height", chartSize)
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

    public updateHeatMap(heatMap: HeatMapData) {
        this.heatMap = heatMap;
        this.updateHeatMapView()
    }
    public updateHeatMapView() {
        if (this.heatMap == null)
            return;
        this.heatMapChart.selectAll("*").remove();
        this.heatMapChart.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.heatMapChart.attr("width"))
            .attr("height", this.heatMapChart.attr("height"))
            .style("fill", "none")
            .style("stroke", "black");
        let pointWidth = this.heatMapChart.attr("width") / this.heatMap.buckets.length;
        let pointHeight = this.heatMapChart.attr("height") / this.heatMap.buckets[0].length;
        let dots = [];
        let max = 0;
        for (let i = 0; i < this.heatMap.buckets.length; i++) {
            for (let j = 0; j < this.heatMap.buckets[0].length; j++) {
                let val = this.heatMap.buckets[i][j];
                if (val > 0) {
                    dots.push({x: i * pointWidth, y: this.heatMapChart.attr("height") - (j + 1) * pointHeight, v: val});
                    max = Math.max(val, max);
                }
            }
        }
        let colorMap = new ColorMap(1, max);
        if (max > ColorMap.logThreshold)
            colorMap.setLogScale(true);
        this.heatMapChart.selectAll()
            .data(dots)
            .enter()
            .append("rect")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
            .attr("data-val", d => d.v)
            .attr("width", pointWidth)
            .attr("height", pointHeight)
            .style("stroke-width", 0)
            .style("fill", d => colorMap.apply(d.v));
    }

    public applyLAMP() {
        let xBuckets = Math.ceil(this.heatMapChart.attr("width") / Resolution.minDotSize);
        let yBuckets = Math.ceil(this.heatMapChart.attr("height") / Resolution.minDotSize);
        let xColAndRange = {
            min: this.minX,
            max: this.maxX,
            samplingRate: 1.0,
            columnName: "LAMP1",
            bucketCount: xBuckets,
            cdfBucketCount: 0,
            bucketBoundaries: null
        }
        let yColAndRange = {
            min: this.minY,
            max: this.maxY,
            samplingRate: 1.0,
            columnName: "LAMP2",
            bucketCount: yBuckets,
            cdfBucketCount: 0,
            bucketBoundaries: null
        }
        let rr = this.originalTableObject.createLAMPMapRequest(this.controlPointsId, this.selectedColumns, this.controlPoints, ["LAMP1", "LAMP2"]);
        rr.invoke(new LAMPMapReceiver(this.page, rr, this, xColAndRange, yColAndRange));
    }

    public updateRanges(l1: BasicColStats, l2: BasicColStats) {
        let [cpMinX, cpMaxX] = [Math.min(...this.controlPoints.points.map(p => p.x)), Math.max(...this.controlPoints.points.map(p => p.x))];
        let [cpMinY, cpMaxY] = [Math.min(...this.controlPoints.points.map(p => p.y)), Math.max(...this.controlPoints.points.map(p => p.y))];
        this.minX = Math.min(l1.min, cpMinX);
        this.minY = Math.min(l2.min, cpMinY);
        let maxX = Math.max(l1.max, cpMaxX);
        let maxY = Math.max(l2.max, cpMaxY);
        let range = Math.max(maxX - this.minX, maxY - this.minY)
        this.maxX = this.minX + range;
        this.maxY = this.minY + range;
        this.refresh();
        this.heatMapChart.selectAll("*").remove();
        this.applyLAMP();
    }

    private fetchNewRanges() {
        let l1: RangeInfo = new RangeInfo("LAMP1")
        let l2: RangeInfo = new RangeInfo("LAMP2")
        let rr = this.lampTableObject.createRange2DRequest(l1, l2);
        rr.invoke(new LAMPRangeCollector(this.page, rr, this))
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
                .attr("fill", "white")
                .attr("stroke", "black")
                .attr("vector-effect", "non-scaling-stroke")
                .call(d3.drag()
                    .on("drag", (p: Point2D, i: number, circles: Element[]) => {
                        let mouse = d3.mouse(plot.node());
                        mouse[0] = clamp(mouse[0], this.minX, this.minX + range)
                        mouse[1] = clamp(mouse[1], this.minY, this.minY + range)
                        p.x = mouse[0];
                        p.y = mouse[1];
                        d3.select(circles[i])
                            .attr("cx", clamp(mouse[0], this.minX, this.minX + range))
                            .attr("cy", clamp(mouse[1], this.minY, this.minY + range))
                    })
                    .on("end", (p: Point2D, i: number, circles: Element[]) => {
                        this.applyLAMP();
                    })
                )
    }
    private showTable() {
        let page = new FullPage();
        this.getPage().insertAfterMe(page);
        let table = new TableView(this.lampTableObject.remoteObjectId, page);
        page.setDataView(table);
        let rr = this.lampTableObject.createGetSchemaRequest();
        rr.invoke(new TableRenderer(this.page, table, rr, false, new RecordOrder([])));
    }
}

export class LAMPDialog extends Dialog {
    public static maxNumSamples = 100;

    constructor(private selectedColumns: string[], private page: FullPage,
                private schema: Schema, private remoteObject: RemoteTableObject) {
        super("LAMP");
        this.addTextField("numSamples", "No. control points", "Integer", "15");
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
                rr = this.remoteObject.createSampledControlPointsRequest(numSamples, this.selectedColumns);
                break;
            }
            case "Category centroids": {
                rr = this.remoteObject.createCategoricalCentroidsControlPointsRequest(category, this.selectedColumns);
                break;
            }
            default: {
                this.page.reportError(`${selection} is not implemented.`);
            }
        }

        let newPage = new FullPage();
        this.page.insertAfterMe(newPage);

        switch (projection) {
            case "MDS": {
                rr.invoke(new ControlPointsProjector(newPage, rr, this.remoteObject, this.selectedColumns));
                break;
            }
            default: {
                this.page.reportError(`${projection} is not implemented.`);
            }
        }
    }
}

class ControlPointsProjector extends RemoteTableRenderer {
    constructor(page, operation, private tableObject: RemoteTableObject, private selectedColumns) {
        super(page, operation, "Sampling control points");
    }

    onCompleted() {
        super.finished();
        if (this.remoteObject == null)
            return;
        let rr = this.tableObject.createMDSProjectionRequest(this.remoteObject.remoteObjectId);
        rr.invoke(new ControlPointsRenderer(this.page, rr, this.tableObject, this.remoteObject.remoteObjectId, this.selectedColumns));
    }
}

class ControlPointsRenderer extends RpcReceiver<PointSet2D> {
    private controlPointsView: ControlPointsView;

    constructor(page, operation, tableObject, controlPointsId, private selectedColumns) {
        super(page.progressManager.newProgressBar(operation, "Projecting control points"),
            page.getErrorReporter())
        this.controlPointsView = new ControlPointsView(tableObject, page, controlPointsId, this.selectedColumns)
    }

    public onNext(result: PointSet2D) {
        super.finished();
        if (result != null)
            this.controlPointsView.updateControlPoints(result);
    }
}

class LAMPMapReceiver extends RemoteTableRenderer {
    constructor(page, operation, private cpView: ControlPointsView, private xColAndRange, private yColAndRange) {
        super(page, operation, "Computing LAMP");
    }

    onCompleted() {
        super.finished();
        this.cpView.updateRemoteTable(this.remoteObject);
        let rr = this.remoteObject.createHeatMapRequest(this.xColAndRange, this.yColAndRange);
        rr.invoke(new LAMPHeatMapReceiver(this.page, rr, this.cpView))
    }
}

class LAMPHeatMapReceiver extends Renderer<HeatMapData> {
    private heatMap: HeatMapData;
    constructor(page, operation, private controlPointsView: ControlPointsView) {
        super(page, operation, "Computing heat map")
    }

    onNext(result: PartialResult<HeatMapData>) {
        super.onNext(result);
        this.heatMap = result.data;
        if (this.heatMap != null) {
            this.controlPointsView.updateHeatMap(this.heatMap)
        }
    }

}

class LAMPRangeCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    constructor(page, operation, private cpView: ControlPointsView) {
        super(page, operation, "Getting LAMP ranges.")
    }

    onNext(result: PartialResult<Pair<BasicColStats, BasicColStats>>) {
        super.onNext(result);
        this.cpView.updateRanges(result.data.first, result.data.second);
    }
}
