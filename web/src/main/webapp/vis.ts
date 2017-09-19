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
     * @param ticks: (Approximate) number of ticks on the axis below the bar.
     * @param base: Base of the numbers that should be displayed if a log scale
                is used. E.g., base = 2 displays all powers of 2 in the range.
     * @param barHeight: Height of the color bar rectangle.
    **/
    constructor(private colorMap: ColorMap,
        size: Size = Resolution.legendSize,
        ticks: number = Math.min(10, colorMap.max - 1),
        base?: number,
        barHeight = 16
    ) {
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("colorLegend");
        this.drawLegend(size, ticks, base, barHeight);
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

    public getScale(width: number, base: number) {
        let scale;
        if (this.colorMap.logScale) {
            if (base == null)
                base = this.colorMap.max > 10000 ? 10 : 2;
            scale = d3.scaleLog()
                .base(base);
        } else
            scale = d3.scaleLinear();

        scale
            .domain([this.colorMap.min, this.colorMap.max])
            .range([0, width]);
        return scale;
    }

    public getAxis(width: number, ticks: number, base: number, bottom: boolean) {
        let scale = this.getScale(width, base);
        if (bottom)
            return d3.axisBottom(scale).ticks(ticks);
        else
            return d3.axisTop(scale).ticks(ticks);
    }

    private drawLegend(size: Size, ticks: number, base: number, barHeight: number) {
        let svg = d3.select(this.topLevel).append("svg")
            .attr("width", size.width)
            .attr("height", size.height);

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
            .attr("width", size.width)
            .attr("height", barHeight)
            .style("fill", "url(#gradient)");

        let axisG = svg.append("g")
            .attr("transform", `translate(0, ${barHeight})`);

        this.textIndicator = svg.append("text").attr("id", "text-indicator")
            .attr("x", "50%")
            .attr("y", "100%")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "bottom");

        let axis = this.getAxis(size.width, ticks, base, true);
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
