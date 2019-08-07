/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {AxisData, AxisDescription} from "./axisData";
import {FilterDescription, RemoteObjectId} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {FullPage} from "../ui/fullPage";
import {Point, Resolution, ViewKind} from "../ui/ui";
import {ChartView} from "./chartView";
import {TrellisShape} from "./dataRangesCollectors";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {TrellisShapeSerialization} from "../datasetView";
import {reorder} from "../util";
import {Dialog, FieldKind} from "../ui/dialog";

/**
 * Point position within a Trellis plot.
 */
interface TrellisPosition {
    /**
     * The plot number; if this is null then the mouse is on no plot.
     */
    plotIndex: number | null;
    /**
     * X coordinate in the plot.
     */
    x: number;
    /**
     * Y coordinate in the plot.
     */
    y: number;
    /**
     * Index of plot on X axis.
     */
    plotXIndex: number;
    /**
     * Index of plot on Y axis.
     */
    plotYIndex: number;
}

/**
 * A base class for all Trellis views
 */
export abstract class TrellisChartView extends ChartView {
    public groupByAxisData: AxisData;
    /**
     * Selection endpoint in canvas.
     */
    protected selectionEnd: Point;
    protected surfaces: PlottingSurface[];
    /**
     * Coordinates of each surface within the canvas.
     */
    protected coordinates: Point[];

    protected constructor(remoteObjectId: RemoteObjectId,
                          rowCount: number,
                          schema: SchemaClass,
                          protected shape: TrellisShape,
                          page: FullPage,
                          viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.selectionEnd = null;
        this.surfaces = null;
    }

    /**
     * Create the surfaces to display the data on.
     * @param onCreation  Method to invoke for each surface created.
     */
    protected createAllSurfaces(
        onCreation: (surface: PlottingSurface) => void): void {
        this.surface = new HtmlPlottingSurface(this.topLevel, this.page, {});

        let created = 0;
        this.surfaces = [];
        this.coordinates = [];
        for (let y = 0; y < this.shape.yNum; y++) {
            for (let x = 0; x < this.shape.xNum; x++) {
                const xCorner = this.surface.leftMargin + x * this.shape.size.width;
                const shortTitle = this.groupByAxisData.bucketDescription(created, this.shape.size.width / 10);
                const title = this.groupByAxisData.bucketDescription(created, 0);
                const canvas = this.surface.getCanvas();
                canvas.append("text")
                    .text(shortTitle)
                    .attr("class", "trellisTitle")
                    .attr("x", xCorner + this.shape.size.width / 2)
                    .attr("y", y * (this.shape.size.height + this.shape.headerHeight)
                        + this.surface.topMargin + (this.shape.headerHeight / 2))
                    .attr("text-anchor", "middle")
                    .attr("dominant-baseline", "middle")
                    .append("title")
                    .text(title);
                const yCorner = y * (this.shape.size.height + this.shape.headerHeight)
                    + this.shape.headerHeight + this.surface.topMargin;
                const surface = this.surface.createChildSurface(xCorner, yCorner, {
                    width: this.shape.size.width,
                    height: this.shape.size.height,
                    topMargin: 0,
                    leftMargin: 0,
                    bottomMargin: 0,
                    rightMargin: 0 });
                this.surfaces.push(surface);
                this.coordinates.push( {
                    x: xCorner,
                    y: yCorner
                } );
                onCreation(surface);
                created++;
                if (created === this.shape.bucketCount)
                    return;
            }
        }
    }

    protected drawAxes(xAxis: AxisDescription, yAxis: AxisDescription): void {
        for (let i = 0; i < this.shape.xNum; i++) {
            const g = this.surface
                .getCanvas()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(
                    ${this.surface.leftMargin + i * this.shape.size.width}, 
                    ${this.surface.topMargin +
                     (this.shape.size.height + this.shape.headerHeight) * this.shape.yNum})`);
            xAxis.draw(g);
        }

        for (let i = 0; i < this.shape.yNum; i++) {
            const g = this.surface.getCanvas()
                .append("g")
                .attr("class", "y-axis")
                .attr("transform", `translate(
                    ${this.surface.leftMargin},
                    ${this.surface.topMargin + this.shape.headerHeight +
                      i * (this.shape.size.height + this.shape.headerHeight)})`);
            yAxis.draw(g);
        }
    }

    public static deserializeShape(ser: TrellisShapeSerialization, page: FullPage): TrellisShape {
        if (ser.xWindows == null || ser.yWindows == null || ser.groupByBucketCount == null)
            return null;
        const size = PlottingSurface.getDefaultCanvasSize(page.getWidthInPixels());
        return {
            xNum: ser.xWindows,
            yNum: ser.yWindows,
            coverage: 1.0,
            bucketCount: ser.groupByBucketCount,
            size: {
                width: size.width / ser.xWindows,
                height: size.height / ser.xWindows
            },
            headerHeight: Resolution.lineHeight
        };
    }

    protected changeGroups(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const maxPlots = Math.round(chartSize.height / (Resolution.lineHeight + Resolution.minTrellisWindowSize)) *
            Math.round(chartSize.width / Resolution.minTrellisWindowSize);
        const dialog = new GroupsDialog(maxPlots);
        dialog.setAction(() => this.doChangeGroups(dialog.getGroupCount()));
        dialog.show();
    }

    protected abstract doChangeGroups(groupCount: number): void;

    /**
     * Returns the current selection index if the current selection falls within a single plot.
     * Otherwise it returns null.
     */
    protected selectionIsLocal(): number | null {
        if (this.selectionOrigin == null)
            return null;

        const origin = this.canvasToChart(this.selectionOrigin);
        const op = this.position(origin.x, origin.y);
        if (op.plotIndex == null)
            return null;

        const position = d3mouse(this.surface.getChart().node());
        const ep = this.position(position[0], position[1]);
        if (ep.plotIndex == null)
            return null;

        if (op.plotIndex === ep.plotIndex)
            return op.plotIndex;
        else
            return null;
    }

    protected dragStart(): void {
        this.dragStartRectangle();
    }

    protected selectSurfaces(start: number, end: number): void {
        for (let index = 0; index < this.shape.bucketCount; index++) {
            const selected = index >= start && index <= end;
            if (index < this.surfaces.length)
                this.surfaces[index].select(selected);
        }
    }

    private selectionWasLocal: boolean = false;
    protected dragMove(): boolean {
        if (this.selectionIsLocal() != null) {
            if (!this.selectionWasLocal)
                this.unselectAllSurfaces();
            this.selectionWasLocal = true;
        } else {
            if (this.selectionWasLocal) {
                // Hide the selection rectangle
                this.selectionRectangle
                    .attr("width", 0)
                    .attr("height", 0);
            }

            this.selectionWasLocal = false;
            // Find the top-left and bottom-right corners of the selection
            const position = d3mouse(this.surface.getChart().node());
            const [x0, x1] = reorder(this.selectionOrigin.x, position[0]);
            const [y0, y1] = reorder(this.selectionOrigin.y, position[1]);
            const posUp = this.position(x0, y0);
            const posDown = this.position(x1, y1);
            if (posUp.plotIndex == null || posDown.plotIndex == null)
                return;
            this.selectSurfaces(posUp.plotIndex, posDown.plotIndex);
        }
        return this.dragMoveRectangle();
    }

    protected unselectAllSurfaces(): void {
        this.selectSurfaces(1, 0); // empty range
    }

    protected abstract selectionCompleted(): void;

    protected dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionEnd = { x: position[0], y: position[1] };
        this.selectionCompleted();
        this.unselectAllSurfaces();
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        return true;
    }

    /**
     * After the selection is completed it returns the description of the filter to apply on
     * the groupBy axis.  Returns null if the selection is not well-defined.
     */
    protected getGroupBySelectionFilter(): FilterDescription {
        if (this.selectionIsLocal() != null)
            return null;
        const end = this.canvasToChart(this.selectionEnd);
        const [x0, x1] = reorder(this.selectionOrigin.x, end.x);
        const [y0, y1] = reorder(this.selectionOrigin.y, end.y);
        const posUp = this.position(x0, y0);
        const posDown = this.position(x1, y1);
        if (posUp.plotIndex == null || posDown.plotIndex == null)
            return null;

        const min = this.groupByAxisData.bucketBoundaries(posUp.plotIndex).getMin();
        const max = this.groupByAxisData.bucketBoundaries(posDown.plotIndex).getMax();
        return {
            min: min.getNumber(),
            max: max.getNumber(),
            minString: min.getString(),
            maxString: max.getString(),
            cd: this.groupByAxisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
    }

    /**
     * Given a position within the chart returns information about the plot
     * where the position falls.
     * @param chartX  X coordinate within chart.
     * @param chartY  Y coordinate within chart.
     */
    protected position(chartX: number, chartY: number): TrellisPosition {
        // Find out which plot we are in.
        const xIndex = Math.floor(chartX / this.shape.size.width);
        const yIndex = Math.floor(chartY / (this.shape.size.height + this.shape.headerHeight));
        let plotIndex = yIndex * this.shape.xNum + xIndex;
        chartX -= xIndex * this.shape.size.width;
        chartY -= yIndex * (this.shape.size.height + this.shape.headerHeight) + this.shape.headerHeight;

        if (xIndex < 0 || plotIndex < 0 || plotIndex >= this.shape.bucketCount)
            plotIndex = null;
        return { plotIndex: plotIndex, x: chartX, y: chartY,
            plotXIndex: xIndex, plotYIndex: yIndex };
    }

    /**
     * Computes the mouse position in a Trellis plot.
     */
    protected mousePosition(): TrellisPosition {
        const position = d3mouse(this.surface.getChart().node());
        return this.position(position[0], position[1]);
    }
}

/**
 * A dialog that queries the user about the number of groups to use.
 */
export class GroupsDialog extends Dialog {
    constructor(protected maxGroups: number) {
        super("Set groups", "Change the number of groups.");
        const groups = this.addTextField("groups", "Number of groups:", FieldKind.Integer, null,
            "The number of groups to use; must be between 1 and " + maxGroups);
        groups.min = "1";
        groups.max = maxGroups.toString();
        groups.required = true;
        this.setCacheTitle("GroupsDialog");
    }

    /**
     * Returns null if the value is not acceptable.
     */
    public getGroupCount(): number {
        const groups = this.getFieldValueAsInt("groups");
        if (groups == null)
            return groups;
        if (groups < 1 || groups > this.maxGroups)
            return null;
        return groups;
    }
}
