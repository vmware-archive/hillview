import {Plot} from "./plot";
import {D3SvgElement, Point, Rectangle, Resolution} from "./ui";
import {mouse as d3mouse} from "d3-selection";
import {drag as d3drag} from "d3-drag";
import {HtmlPlottingSurface} from "./plottingSurface";

/**
 * A Plot for the legend of a chart.
 */
export abstract class LegendPlot<D> extends Plot<D> {
    protected hilightRect: D3SvgElement;  // used to highlight
    protected legendSelectionRectangle: D3SvgElement;  // used to select
    protected readonly height = 16;
    protected drawn: boolean;
    protected dragging: boolean;
    protected dragMoved: boolean;
    public readonly width: number;
    protected x: number;
    protected y: number;
    protected selectionCompleted: (xl: number, xr: number) => void;

    /**
     * Coordinates of mouse within canvas.
     */
    protected selectionOrigin: Point;
    protected legendRect: Rectangle;

    protected constructor(surface: HtmlPlottingSurface, onSelectionCompleted: (xl: number, xr: number) => void) {
        super(surface);
        this.selectionCompleted = onSelectionCompleted;
        this.drawn = false;
        this.dragging = false;
        this.dragMoved = false;
        this.width = Resolution.legendBarWidth;
        if (this.width > this.getChartWidth())
            this.width = this.getChartWidth();
        this.x = (this.getChartWidth() - this.width) / 2;
        surface.topLevel.tabIndex = 1;  // necessary to get key events?
        surface.topLevel.onkeydown = (e) => this.keyDown(e);
    }

    protected createRectangle() {
        this.legendRect = new Rectangle(
            { x: this.x, y: this.y },
            { width: this.width, height: this.height });
    }

    public draw() {
        const canvas = this.plottingSurface.getCanvas();
        this.hilightRect = canvas.append("rect")
            .attr("class", "dashed")
            .attr("height", this.height)
            .attr("x", 0)
            .attr("y", 0)
            .attr("stroke-dasharray", "5,5")
            .attr("stroke", "cyan")
            .attr("fill", "none");
        this.legendSelectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);
        const legendDrag = d3drag()
            .on("start", () => this.dragLegendStart())
            .on("drag", () => this.dragLegendMove())
            .on("end", () => this.dragLegendEnd());
        canvas.call(legendDrag);
        canvas.tabIndex = 1;  // seems to be necessary to get keyboard events
        this.drawn = true;
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "Escape")
            this.cancelDrag();
    }

    protected cancelDrag(): void {
        this.dragging = false;
        this.dragMoved = false;
        this.hideSelectionRectangle();
    }

    // dragging in the legend
    protected dragLegendStart(): void {
        this.dragging = true;
        this.dragMoved = false;
        const position = d3mouse(this.plottingSurface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    protected hideSelectionRectangle(): void {
        this.legendSelectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    protected dragLegendMove(): void {
        if (!this.dragging || !this.drawn)
            return;
        this.dragMoved = true;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.plottingSurface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.legendRect.height();

        if (width < 0) {
            ox = x;
            width = -width;
        }
        this.legendSelectionRectangle
            .attr("x", ox)
            .attr("width", width)
            .attr("y", this.legendRect.upperLeft().y)
            .attr("height", height);

        // Prevent the selection from spilling out of the legend itself
        if (ox < this.legendRect.origin.x) {
            const delta = this.legendRect.origin.x - ox;
            if (width > delta) {
                this.legendSelectionRectangle
                    .attr("x", this.legendRect.origin.x)
                    .attr("width", width - delta);
            } else {
                this.hideSelectionRectangle();
            }
        } else if (ox + width > this.legendRect.lowerRight().x) {
            const delta = ox + width - this.legendRect.lowerRight().x;
            if (width > delta) {
                this.legendSelectionRectangle.attr("width", width - delta);
            } else {
                this.hideSelectionRectangle();
            }
        }
    }

    protected dragLegendEnd(): boolean {
        if (!this.dragging || !this.dragMoved || !this.drawn)
            return false;
        this.dragging = false;
        this.dragMoved = false;
        this.hideSelectionRectangle();
        const position = d3mouse(this.plottingSurface.getCanvas().node());
        const x = position[0];
        if (this.selectionCompleted != null)
            this.selectionCompleted(this.selectionOrigin.x - this.legendRect.lowerLeft().x,
                x - this.legendRect.lowerLeft().x);
        return true;
    }

    public legendRectangle(): Rectangle {
        return this.legendRect;
    }
}