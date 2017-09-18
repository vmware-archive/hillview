import d3 = require("d3");
import {Size, IElement} from "./ui";

export class ColorMap {
    public logScale: boolean;
    constructor(public max, public min = 1) {
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

    // Apply the base colormap. x is in the range [0, 1].
    private map(x: number) {
        return d3.interpolateWarm(x);
    }

    public getScale(width: number, base: number) {
        let scale;
        if (this.logScale) {
            if (base == null)
                base = this.max > 10000 ? 10 : 2;
            scale = d3.scaleLog()
                .base(base);
        } else
            scale = d3.scaleLinear();

        scale
            .domain([this.min, this.max])
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

    /**
     * Draw a color legend for this ColorMap with the specified parameters.
     * The legend has an element id'ed with #text-indicator, that can be
     * selected and updated to indicate the value.
     * @param element: Element to draw the color legend in. The width attribute
                of this element is used.
     * @param ticks: (Approximate) number of ticks on the axis below the bar.
     * @param base: Base of the numbers that should be displayed if a log scale
                is used. E.g., base = 2 displays all powers of 2 in the range.
     * @param barHeight: Height of the color bar rectangle.
     * Assumes there are margins outside this element.
    **/
    public drawLegend(element, ticks: number = Math.min(10, this.max - 1), base?: number, barHeight = 16) {
        let size: Size = {width: element.attr("width"), height: element.attr("height")};

        let gradient = element.append('defs')
            .append('linearGradient')
            .attr('id', 'gradient')
            .attr('x1', '0%')
            .attr('y1', '0%')
            .attr('x2', '100%')
            .attr('y2', '0%')
            .attr('spreadMethod', 'pad');
        for (let i = 0; i <= 100; i += 4)
            gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", this.map(i / 100))
                .attr("stop-opacity", 1)

        element.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", size.width)
            .attr("height", barHeight)
            .style("fill", "url(#gradient)");

        let axisG = element.append("g")
            .attr("transform", `translate(0, ${barHeight})`);

        let textIndicator = element.append("text").attr("id", "text-indicator")
            .attr("x", size.width / 2)
            .attr("y", size.height)
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "bottom");

        let axis = this.getAxis(size.width, ticks, base, true);
        axisG.call(axis);
    }
}

export class DataRange implements IElement {
    private topLevel: Element;

    constructor(position: number, count: number, totalCount: number) {
            this.topLevel = document.createElementNS("http://www.w3.org/2000/svg", "svg")
            this.topLevel.classList.add("dataRange");
            // If the range represents < 1 % of the total count, use 1% of the bar's width.
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
