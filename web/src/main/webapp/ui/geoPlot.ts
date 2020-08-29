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
import {
    geoAlbersUsa,
    geoAzimuthalEqualArea,
    geoAzimuthalEquidistant,
    geoConicEqualArea, geoConicEquidistant, geoContains,
    geoEqualEarth, geoEquirectangular,
    geoGnomonic, geoMercator,
    geoNaturalEarth1,
    geoOrthographic, GeoPath,
    geoPath,
    GeoProjection,
    geoStereographic, geoTransverseMercator
} from "d3-geo";
import {MapAndColumnRepresentation} from "../javaBridge";
import {Feature, GeometryObject} from "geojson";

export class Box {
    bounds: [[number, number], [number, number]];
    property: string;
    value: number | null;
}

export class GeoPlot extends Plot<MapAndColumnRepresentation> {
    protected aggregate: Map<String, number> | null = null;
    protected geoGenerator: GeoPath | null = null;
    protected projection: GeoProjection | null = null;

    public constructor(surface: PlottingSurface,
                       protected colorMap: ColorMap) {
        super(surface);
    }

    public setMap(data: MapAndColumnRepresentation): void {
        this.data = data;
    }

    public setData(aggregate: Map<String, number>): void {
        this.aggregate = aggregate;
    }

    getProjection(): GeoProjection {
        switch (this.data.projection) {
            case "geoAlbersUsa":
                return geoAlbersUsa();
            case "geoAzimuthalEqualArea":
                return geoAzimuthalEqualArea();
            case "geoAzimuthalEquidistant":
                return geoAzimuthalEquidistant();
            case "geoGnomonic":
                return geoGnomonic();
            case "geoStereographic":
                return geoStereographic();
            case "geoEqualEarth":
                return geoEqualEarth();
            case "geoConicEqualArea":
                return geoConicEqualArea();
            case "geoConicEquidistant":
                return geoConicEquidistant();
            case "geoEquirectangular":
                return geoEquirectangular();
            case "geoMercator":
                return geoMercator();
            case "geoTransverseMercator":
                return geoTransverseMercator();
            case "geoNaturalEarth1":
                return geoNaturalEarth1();
            case "geoOrthographic":
            default:
                return geoOrthographic();
        }
    }
    
    draw(): void {
        const chart = this.plottingSurface.getChart();
        this.projection = this.getProjection()
            .fitExtent([[0, 0], [this.getChartWidth(), this.getChartHeight()]], this.data.data);
        this.geoGenerator = geoPath().projection(this.projection);
        chart.selectAll('path')
            .data(this.data.data.features)
            .enter()
            .append('path')
            .attr('d', this.geoGenerator)
            .attr("fill", (d: Feature<GeometryObject>) => {
                const prop = d.properties[this.data.property];
                return this.color(prop);
            })
            .attr("stroke", "#aaa");
    }

    public get(x: number, y: number): Box | null {
        if (this.projection == null)
            return null;
        let coords: [number, number] | null = this.projection.invert!([x, y]);
        if (coords == null)
            return null;
        for (const f of this.data.data.features) {
            if (geoContains(f, coords)) {
                const bounds = this.geoGenerator!.bounds(f);
                const prop = f.properties[this.data.property];
                const value = this.count(prop);
                return {
                    bounds,
                    property: prop,
                    value
                };
            }
        }
        return null;
    }

    protected color(property: string): string {
        const count = this.count(property);
        if (count == null)
            return "#ddd";
        return this.colorMap(count);
    }

    protected count(property: string): number | null {
        if (this.aggregate == null)
            return null;
        return this.aggregate.get(property);
    }
}
