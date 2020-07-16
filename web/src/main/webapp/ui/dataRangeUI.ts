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

import {select as d3select} from "d3-selection";
import {formatNumber, percentString, px} from "../util";
import {IElement} from "./ui";

/**
 * A horizontal rectangle that displays a data range within an interval 0-max.
 */
export class DataRangeUI implements IElement {
    private readonly topLevel: Element;
    public static readonly width = 80;  // pixels

    /**
     * @param position: Beginning of data range.
     * @param count:    Size of data range.
     * @param totalCount: Maximum value of interval.
     */
    constructor(position: number, count: number, totalCount: number) {
        this.topLevel = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        this.topLevel.classList.add("dataRange");
        this.topLevel.setAttribute("width", px(DataRangeUI.width));
        // If the range represents < 1 % of the total count, use 1% of the
        // bar's width, s.t. it is still visible.
        const w = Math.max(0.01, count / totalCount);
        let x = position / totalCount;
        if (x + w > 1)
            x = 1 - w;
        const label = w.toString() + "%";
        d3select(this.topLevel)
            .append("g")
            .append("svg:title")
            .text(
                formatNumber(position) + "+" + formatNumber(count) + "/" + formatNumber(totalCount) + "\n" +
                percentString(position / totalCount) + "+" + percentString(count / totalCount),
            );
        d3select(this.topLevel).select("g")
            .append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("fill", "lightgray")
            .attr("width", 1)
            .attr("height", 1);
        d3select(this.topLevel).select("g")
            .append("rect")
            .attr("x", x.toString() + "%")
            .attr("y", 0)
            .attr("fill", "black")
            .attr("width", label)
            .attr("height", 1);
    }

    public getDOMRepresentation(): Element {
        return this.topLevel;
    }
}
