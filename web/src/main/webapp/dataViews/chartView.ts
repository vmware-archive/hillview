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

import {BigTableView} from "../tableTarget";
import {DataRange, RemoteObjectId} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {FullPage} from "../ui/fullPage";
import {D3SvgElement, Point, ViewKind} from "../ui/ui";
import {TextOverlay} from "../ui/textOverlay";
import {PlottingSurface} from "../ui/plottingSurface";
import {TopMenu} from "../ui/menu";
import {drag as d3drag} from "d3-drag";
import {mouse as d3mouse} from "d3-selection";

/**
 * A ChartView is a common base class for many views that
 * display charts.
 */
export abstract class ChartView extends BigTableView {
    /**
     * True while the mouse is being dragged.
     */
    protected dragging: boolean;
    /**
     * True if the mouse has been moved after starting dragging.
     */
    protected moved: boolean;
    /**
     * Coordinates of mouse within canvas.
     */
    protected selectionOrigin: Point;
    /**
     * Rectangle in canvas used to display the current selection.
     */
    protected selectionRectangle: D3SvgElement;
    /**
     * Describes the data currently pointed by the mouse.
     */
    protected pointDescription: TextOverlay;
    /**
     * The main surface on top of which the image is drawn.
     * There may exist other surfaces as well besides this one.
     */
    protected surface: PlottingSurface;
    /**
     * Top-level menu.
     */
    protected menu: TopMenu;

    protected constructor(remoteObjectId: RemoteObjectId,
                          rowCount: number,
                          schema: SchemaClass,
                          page: FullPage,
                          viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.dragging = false;
        this.moved = false;
        this.selectionOrigin = null;
        this.selectionRectangle = null;
        this.pointDescription = null;
        this.surface = null;
    }

    protected setupMouse(): void {
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.topLevel.tabIndex = 1;  // seems to be necessary to get keyboard events
        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        const canvas = this.surface.getCanvas();
        canvas.call(drag)
            .on("mousemove", () => this.onMouseMove())
            .on("mouseenter", () => this.onMouseEnter())
            .on("mouseleave", () => this.onMouseLeave());
        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "Escape")
            this.cancelDrag();
    }

    protected onMouseEnter(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(true);
        }
    }

    protected onMouseLeave(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(false);
        }
    }

    protected cancelDrag(): void {
        this.dragging = false;
        this.moved = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    /**
     * Converts a point coordinate in canvas to a point coordinate in the chart surface.
     */
    public canvasToChart(point: Point): Point {
        return { x: point.x - this.surface.leftMargin, y: point.y - this.surface.topMargin };
    }

    protected abstract onMouseMove(): void;

    protected dragStart(): void {
        this.dragging = true;
        this.moved = false;
    }

    protected dragStartRectangle(): void {
        this.dragging = true;
        this.moved = false;
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    protected makeToplevelDiv(): HTMLDivElement {
        const div = document.createElement("div");
        this.topLevel.appendChild(div);
        return div;
    }

    /**
     * Get the data range for the X axis only if the x axis has the right name.
     * Returns null if there is no X axis or "column"
     * does not match the column plotted on the X axis.
     * @param column  Column name expected.
     */
    public getXAxisRange(column: string): DataRange | null {
        return null;
    }

    /**
     * Get the data range for the Y axis only if the y axis has the right name.
     * Returns null if there is no Y axis or "column"
     * does not match the column plotted on the Y axis.
     * @param column  Column name expected.
     */
    public getYAxisRange(column: string): DataRange | null {
        return null;
    }

    /**
     * An implementation of dragMove that is suitable when
     * we track the precise mouse position.
     */
    protected dragMoveRectangle(): boolean {
        this.onMouseMove();
        if (!this.dragging) {
            return false;
        }
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        let width = x - ox;
        let height = y - oy;

        if (width < 0) {
            ox = x;
            width = -width;
        }
        if (height < 0) {
            oy = y;
            height = -height;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", oy)
            .attr("width", width)
            .attr("height", height);
        return true;
    }

    protected dragMove(): boolean {
        this.onMouseMove();
        if (!this.dragging) {
            return false;
        }
        this.moved = true;
        return true;
    }

    /**
     * Returns true if there has been some interesting dragging.
     */
    protected dragEnd(): boolean {
        if (!this.dragging || !this.moved) {
            return false;
        }
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        return true;
    }
}
