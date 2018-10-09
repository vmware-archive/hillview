/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {mouse as d3mouse} from "d3-selection";
import {
    RemoteObjectId
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {CDFPlot} from "../ui/CDFPlot";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {D3SvgElement, Resolution, ViewKind} from "../ui/ui";
import {ChartView} from "./chartView";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase extends ChartView {
    protected summary: HTMLElement;
    protected cdfDot: D3SvgElement;
    protected cdfPlot: CDFPlot;
    protected chartDiv: HTMLDivElement;

    protected constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage, viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";

        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);
        this.chartDiv.style.display = "flex";
        this.chartDiv.style.flexDirection = "column";

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    protected abstract showTable(): void;
    public abstract resize(): void;

    /**
     * Dragging started in the canvas.
     */
    public dragStart(): void {
        super.dragStart();
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    /**
     * The mouse moved in the canvas.
     */
    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.surface.getChartHeight();

        if (width < 0) {
            ox = x;
            width = -width;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", this.surface.topMargin)
            .attr("width", width)
            .attr("height", height);
        return true;
    }
}

/**
 * A dialog that queries the user about the number of buckets to use.
 */
export class BucketDialog extends Dialog {
    constructor() {
        super("Set buckets", "Change the number of buckets (bars) used to display the histogram.");
        const input = this.addTextField("n_buckets", "Number of buckets:", FieldKind.Integer, null,
            "The number of buckets to use.");
        input.min = "1";
        input.max = Resolution.maxBucketCount.toString();
        this.setCacheTitle("BucketDialog");
    }

    public getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}
