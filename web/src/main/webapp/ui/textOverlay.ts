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

import {Resolution} from "./ui";
import {truncate} from "../util";
import {PlottingSurface} from "./plottingSurface";

/**
 * This class is used to display a small semi-transparent rectangle that
 * contains several key=value pairs.
 * The rectangle is slightly transparent.
 */
export class TextOverlay {
    /**
     * An SVG rectangle.
     */
    private rect: any;
    private height: number;
    private lines: any[];

    /**
     * Create a textOverlay.
     * TODO: change parent to be a PlottingSurface
     * @param parent     Parent is really d3 svg element.
     * @param keys       Keys whose values will be updated.
     * @param maxLength  Maximum rendered key length.
     */
    constructor(private parent: any, private keys: string[], maxLength: number) {
        this.height = keys.length * Resolution.lineHeight;
        this.rect = this.parent.append("rect")
            .attr("height", this.height)
            .attr("width", 100)
            .attr("fill", "rgba(255, 255, 255, 0.9)");
        this.lines = [];
        for (let index in keys) {
            keys[index] = truncate(keys[index], maxLength);
            this.lines.push(
                this.parent.append("text")
                    .attr("dominant-baseline", "text-before-edge")
                    .attr("text-anchor", "left"));
        }
    }

    show(visible: boolean): void {
        this.rect.attr("visibility", visible ? "visible" : "hidden");
        for (let l of this.lines)
            l.attr("visibility", visible ? "visible" : "hidden");
    }

    /**
     * Update the contents of the overlay.
     * @param {string[]} values  Values corresponding to the keys.
     * @param {number} x         X screen coordinate.
     * @param {number} y         Y screen coordinate.
     */
    update(values: string[], x: number, y: number): void {
        let maxWidth = 0;

        // compute width
        for (let index = 0; index < values.length; index++)
            maxWidth = Math.max(maxWidth, this.lines[index].node().getBBox().width);

        // If too close to the margin move it a bit
        if (window.innerWidth < x + maxWidth)
            x -= maxWidth;
        // TODO: make these depend on dynamic values
        if (PlottingSurface.canvasHeight - PlottingSurface.bottomMargin < y + this.height)
            y -= this.height;

        let index = 0;
        let crtY = y;
        for (let v of values) {
            this.lines[index]
                .text(this.keys[index] + " = " + v)
                .attr("x", x)
                .attr("y", crtY);
            crtY += Resolution.lineHeight;
            index++;
        }
        this.rect
            .attr("x", x)
            .attr("y", y)
            .attr("width", maxWidth + 10);
    }
}
