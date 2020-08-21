/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {ColorMap} from "../util";
import {geoEquirectangular, geoPath} from "d3-geo";
import {SimpleFeatureCollection} from "../javaBridge";

export class GeoPlot extends Plot<SimpleFeatureCollection> {
    public constructor(surface: PlottingSurface,
                       protected colorMap: ColorMap) {
        super(surface);
    }

    public setData(data: SimpleFeatureCollection): void {
        this.data = data;
    }

    draw(): void {
        const canvas = this.plottingSurface.getCanvas();
        var projection = geoEquirectangular()
            .fitExtent([[0, 0], [this.getChartWidth(), this.getChartHeight()]], this.data);

        var geoGenerator = geoPath().projection(projection);
        var u = canvas
            .selectAll('path')
            .data(this.data.features);
        u.enter()
            .append('path')
            .attr('d', geoGenerator)
            .attr("fill", "#ddd")
            .attr("stroke", "#aaa");
    }
}
