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

import {AxisData} from "./axisData";
import {RemoteObjectId} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {FullPage} from "../ui/fullPage";
import {ViewKind} from "../ui/ui";
import {ChartView} from "./chartView";
import {TrellisShape} from "./dataRangesCollectors";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {mouse as d3mouse} from "d3-selection";
import {TrellisShapeSerialization} from "../datasetView";

/**
 * The mouse position in a Trellis plot.
 */
interface TrellisMousePosition {
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
    protected groupByAxisData: AxisData;

    protected constructor(remoteObjectId: RemoteObjectId,
                          rowCount: number,
                          schema: SchemaClass,
                          protected shape: TrellisShape,
                          page: FullPage,
                          viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
    }

    /**
     * Create the surfaces to display the data on.
     * @param onCreation  Method to invoke for each surface created.
     */
    protected createSurfaces(onCreation: (surface: PlottingSurface) => void): void {
        this.surface = new HtmlPlottingSurface(this.topLevel, this.page);
        this.surface.create();

        for (let y = 0; y < this.shape.yNum; y++) {
            for (let x = 0; x < this.shape.xNum; x++) {
                const surface = this.surface.createChildSurface(
                    this.surface.leftMargin + x * this.shape.size.width,
                    y * this.shape.size.height + this.surface.topMargin);
                surface.setSize(this.shape.size);
                surface.setMargins(0, 0, 0, 0);
                onCreation(surface);
            }
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
            }
        };
    }

    /**
     * Computes the mouse position in a Trellis plot.
     */
    protected mousePosition(): TrellisMousePosition {
        const position = d3mouse(this.surface.getChart().node());
        let mouseX = position[0];
        let mouseY = position[1];

        // Find out which plot we are in.
        const xIndex = Math.floor(mouseX / this.shape.size.width);
        const yIndex = Math.floor(mouseY / this.shape.size.height);
        let plotIndex = yIndex * this.shape.xNum + xIndex;
        mouseX -= xIndex * this.shape.size.width;
        mouseY -= yIndex * this.shape.size.height;

        if (plotIndex < 0 || plotIndex >= this.shape.bucketCount)
            plotIndex = null;
        return { plotIndex: plotIndex, x: mouseX, y: mouseY, plotXIndex: xIndex, plotYIndex: yIndex };
    }
}
