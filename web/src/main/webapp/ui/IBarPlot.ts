/**
 * Interface for a class that can draw data in a Histogram (Bar).
 */
import {Histogram} from "../javaBridge";
import {AxisData} from "../dataViews/axisData";
import {D3Scale} from "./ui";

export interface IBarPlot {
    setHistogram(bars: Histogram, samplingRate: number,
                 axisData: AxisData, maxYAxis: number | null, isPrivate: boolean): void;
    draw(): void;
    getYScale(): D3Scale;

    /**
     * Given a X coordinate return the size of the bar at that coordinate with a confidence interval.
     */
    get(x: number): [number, number];
}