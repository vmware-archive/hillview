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

import {BigTableView} from "../modules";
import {BucketsInfo, IColumnDescription, RangeFilterArrayDescription, RecordOrder, RemoteObjectId} from "../javaBridge";
import {DisplayName, SchemaClass} from "../schemaClass";
import {FullPage, PageTitle} from "../ui/fullPage";
import {D3SvgElement, DragEventKind, Point, Resolution, ViewKind} from "../ui/ui";
import {TextOverlay} from "../ui/textOverlay";
import {PlottingSurface} from "../ui/plottingSurface";
import {TopMenu} from "../ui/menu";
import {drag as d3drag} from "d3-drag";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {AxisData} from "./axisData";
import {Dialog} from "../ui/dialog";
import {NextKReceiver, TableView} from "../modules";

/**
 * A ChartView is a common base class for many views that
 * display charts.
 */
export abstract class ChartView<D> extends BigTableView {
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
    /**
     * Data that is being displayed.
     */
    protected data: D;

    protected constructor(remoteObjectId: RemoteObjectId,
                          rowCount: number,
                          schema: SchemaClass,
                          page: FullPage,
                          viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart-page";

        this.dragging = false;
        this.moved = false;
        this.selectionOrigin = null;
        this.selectionRectangle = null;
        this.pointDescription = null;
        this.surface = null;

        this.page.registerDropHandler("XAxis", (p) => this.replaceAxis(p, "XAxis"));
        this.page.registerDropHandler("YAxis", (p) => this.replaceAxis(p, "YAxis"));
        this.page.registerDropHandler("GAxis", (p) => this.replaceAxis(p, "GAxis"));
    }

    protected showTable(columns: IColumnDescription[], provenance: string): void {
        const newPage = this.dataset.newPage(new PageTitle("Table", provenance), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder(
            columns.map(c => { return { columnDescription: c, isAscending: true }}));
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
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
        this.hideSelectionRectangle();
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

    /**
     * Get the range of the specified axis.
     * @param dragEvent  A drag event kind; some of these correspond to dragging axes.
     */
    public getAxisData(dragEvent: DragEventKind): AxisData | null {
        return null;
    }

    /**
     * Called when an event caused by the dragging of an axis happens.
     * @param pageId     Source page where the drag data comes from.
     * @param eventKind  Drag event kind.
     */
    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {}

    public static canDragAxisFromTo(source: ViewKind, destination: ViewKind): boolean {
        if (source.indexOf("Heatmap") >= 0)
            return destination.indexOf("Heatmap") >= 0;
        if (source.indexOf("Histogram") >= 0)
            return destination.indexOf("Histogram") >= 0;
        return false;
    }

    protected chooseTrellis(columns: DisplayName[]): void {
        if (columns.length === 0) {
            this.page.reportError("No acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a column to group on.");
        dialog.addColumnSelectField("column", "column", columns, null,
            "The column that will be used to group on.");
        dialog.setAction(() => this.showTrellis(dialog.getColumnName("column")));
        dialog.show();
    }

    protected abstract showTrellis(colName: DisplayName): void;

    public getSourceAxisRange(sourcePageId: string, dragEvent: DragEventKind): BucketsInfo | null {
        const page = this.dataset.findPage(Number(sourcePageId));
        if (page == null)
            return;
        const source = page.getDataView() as ChartView<D>;
        if (source == null)
            return;

        // Check compatibility
        const thisView = this.page.getDataView();
        if (thisView == null)
            return null;
        if (!ChartView.canDragAxisFromTo(source.viewKind, thisView.viewKind)) {
            this.page.reportError("Cannot drag axis between these views");
            return null;
        }

        const sourceAxis = source.getAxisData(dragEvent);
        if (sourceAxis == null)
            return null;

        // check that the two axes are on the same column
        const myAxis = this.getAxisData(dragEvent);
        if (myAxis == null)
            return null;

        if (sourceAxis.description !== myAxis.description) {
            this.page.reportError("Axis is on different columns");
            return null;
        }

        return sourceAxis.dataRange;
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

    protected hideSelectionRectangle(): void {
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    /**
     * Returns true if there has been some interesting dragging.
     */
    protected dragEnd(): boolean {
        if (!this.dragging || !this.moved) {
            return false;
        }
        this.dragging = false;
        this.hideSelectionRectangle();
        return true;
    }

    /**
     * Create a 2D filter that selects the specified rectangle of data.
     * The mouse coordinates xl, xr, yl, yr are within the canvas.
     */
    protected filterSelectionRectangle(xl: number, xr: number, yl: number, yr: number,
                                       xAxisData: AxisData, yAxisData: AxisData):
        RangeFilterArrayDescription {
        if (xAxisData.axis == null ||
            yAxisData.axis == null) {
            return null;
        }

        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        yl -= this.surface.topMargin;
        yr -= this.surface.topMargin;
        const xRange = xAxisData.getFilter(xl, xr);
        const yRange = yAxisData.getFilter(yl, yr);
        return {
            filters: [xRange, yRange],
            complement: d3event.sourceEvent.ctrlKey
        };
    }
}
