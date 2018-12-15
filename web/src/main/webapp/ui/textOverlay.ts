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

import {truncate} from "../util";
import {D3SvgElement, Resolution, Size} from "./ui";

/**
 * This class is used to display a small semi-transparent rectangle that
 * contains several key=value pairs.
 * The rectangle is slightly transparent.
 */
export class TextOverlay {
    /**
     * An SVG rectangle.
     */
    private rect: D3SvgElement;
    private readonly height: number;
    private readonly lines: D3SvgElement[];

    /**
     * Create a textOverlay.
     * @param parent     Parent is really d3 svg element.
     * @param parentSize The size of the parent element,
     * @param keys       Keys whose values will be updated.
     * @param maxLength  Maximum rendered key length.
     */
    constructor(private readonly parent: D3SvgElement, private readonly parentSize: Size,
                private readonly keys: string[], maxLength: number) {
        this.height = keys.length * Resolution.lineHeight;
        this.rect = this.parent.append("rect")
            .attr("class", "text-overlay")
            .attr("height", this.height)
            .attr("width", 0)
            .attr("fill", "rgba(255, 255, 255, 0.9)");
        this.lines = [];
        for (let key of keys) {
            key = truncate(key, maxLength);
            this.lines.push(
                this.parent.append("text")
                    .attr("dominant-baseline", "text-before-edge")
                    .attr("text-anchor", "start"));
        }
        this.show(false);
    }

    public show(visible: boolean): void {
        const v = visible ? "visible" : "hidden";
        this.rect.attr("visibility", v);
        for (const l of this.lines)
            l.attr("visibility", v);
    }

    /**
     * Update the contents of the overlay.
     * @param {string[]} values  Values corresponding to the keys.
     * @param {number} x         X screen coordinate.
     * @param {number} y         Y screen coordinate.
     */
    public update(values: string[], x: number, y: number): void {
        let maxWidth = 0;

        // compute width
        for (let ix = 0; ix < values.length; ix++)
            maxWidth = Math.max(maxWidth, this.lines[ix].node().getBBox().width);

        // If too close to the margin move it a bit
        if (this.parentSize.width < x + maxWidth)
            x -= maxWidth;
        if (this.parentSize.height < y + this.height)
            y -= this.height;

        let index = 0;
        let crtY = y;
        for (let v of values) {
            v = truncate(v, 100);
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
