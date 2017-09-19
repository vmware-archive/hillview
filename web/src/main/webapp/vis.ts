import d3 = require("d3");
import {Size, IElement, IHtmlElement, significantDigits, Resolution} from "./ui";
import {ContextMenu} from "./menu";

export class ColorMap {
    public static logThreshold = 50; /* Suggested threshold for when a log-scale should be used. */
    public logScale: boolean;

    public map: (x) => string = d3.interpolateWarm;

    constructor(public min = 0, public max = 1) {}

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
}

export class ColorLegend implements IHtmlElement {
    private topLevel: HTMLElement;
    private gradient: any; // Element that contains the definitions for the colors in the color map
    private textIndicator: any; // Text indicator for the value.

    private onColorMapChange: (ColorMap) => void; // Function that is called to update other elements when the color map changes.
    private contextMenu: ContextMenu;

    /**
     * Make a color legend for the given ColorMap with the specified parameters.
     * @param colorMap: ColorMap to make this legend for.
     * @param size: Size of the legend
     * @param barHeight: Height of the color bar rectangle.
    **/
    constructor(private colorMap: ColorMap,
        private size: Size = Resolution.legendSize,
        private barHeight = 16
    ) {
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("colorLegend");
    }

    public setColorMapChangeEventListener(listener: (ColorMap) => void) {
        this.onColorMapChange = listener;
        if (this.contextMenu == null)
            this.enableContextMenu();
    }

    /**
     * The context menu is added only when a colormap change event listener is set.
     */
    private enableContextMenu() {
        this.contextMenu = new ContextMenu([
            {
                text: "Cool",
                action: () => this.setMap(d3.interpolateCool)
            }, {
                text: "Warm",
                action: () => this.setMap(d3.interpolateWarm)
            }
        ])
        this.topLevel.appendChild(this.contextMenu.getHTMLRepresentation());
        this.topLevel.oncontextmenu = e => {
            e.preventDefault();
            this.contextMenu.move(e.pageX - 1, e.pageY - 1);
            this.contextMenu.show();
        };
    }

    // Set a new (base) color map. Needs to redefine the gradient.
    private setMap(map: (number) => string) {
        this.colorMap.map = map;
        this.gradient.selectAll("*").remove();
        for (let i = 0; i <= 100; i += 4)
            this.gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", this.colorMap.map(i / 100))
                .attr("stop-opacity", 1)
        // Notify the onColorChange listener (redraw the elements with new colors)
        if (this.onColorMapChange != null)
            this.onColorMapChange(this.colorMap);
    }

    private base(): number {
        return this.colorMap.max > 10000 ? 10 : 2
    }

    private ticks(): number {
        return Math.min(this.colorMap.max, 10);
    }

    public getScale() {
        let scale;
        if (this.colorMap.logScale) {
            scale = d3.scaleLog()
                .base(this.base());
        } else
            scale = d3.scaleLinear();
        scale
            .domain([this.colorMap.min, this.colorMap.max])
            .range([0, this.size.width]);
        return scale;
    }

    public getAxis() {
        let scale = this.getScale();
        return d3.axisBottom(scale).ticks(this.ticks());
    }

    public redraw() {
        d3.select(this.topLevel).selectAll("svg").remove();
        let svg = d3.select(this.topLevel).append("svg")
            .attr("width", this.size.width)
            .attr("height", this.size.height);

        this.gradient = svg.append('defs')
            .append('linearGradient')
            .attr('id', 'gradient')
            .attr('x1', '0%')
            .attr('y1', '0%')
            .attr('x2', '100%')
            .attr('y2', '0%')
            .attr('spreadMethod', 'pad');
        for (let i = 0; i <= 100; i += 4)
            this.gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", this.colorMap.map(i / 100))
                .attr("stop-opacity", 1)

        svg.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.size.width)
            .attr("height", this.barHeight)
            .style("fill", "url(#gradient)");

        let axisG = svg.append("g")
            .attr("transform", `translate(0, ${this.barHeight})`);

        this.textIndicator = svg.append("text").attr("id", "text-indicator")
            .attr("x", "50%")
            .attr("y", "100%")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "bottom");

        let axis = this.getAxis();
        axisG.call(axis);
    }

    /* Indicate the given value in the text indicator. */
    public indicate(val: number) {
        if (val == null || isNaN(val))
            this.textIndicator.text("");
        else
            this.textIndicator.text(significantDigits(val));
    }

    public getHTMLRepresentation() {
        return this.topLevel;
    }
}

/**
 * This class displays the relative size of a subsequence within a sequence,
 * in a bar.
 */
export class DataRange implements IElement {
    private topLevel: Element;

    /**
     * @param position: Index where the subsequence starts.
     * @param count: Number of items in the subsequence.
     * @param totalCount: Total number of items in the 'supersequence'.
     */
    constructor(position: number, count: number, totalCount: number) {
            this.topLevel = document.createElementNS("http://www.w3.org/2000/svg", "svg")
            this.topLevel.classList.add("dataRange");
            // If the range represents < 1 % of the total count, use 1% of the
            // bar's width, s.t. it is still visible.
            let w = Math.max(0.01, count / totalCount);
            let x = position / totalCount;
            if (x + w > 1)
            x = 1 - w;
            d3.select(this.topLevel)
                .append("g").append("rect")
                .attr("x", x)
                .attr("y", 0)
                .attr("width", w)
                .attr("height", 1);
        }

    public getDOMRepresentation(): Element {
        return this.topLevel;
    }
}
