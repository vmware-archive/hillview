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
import {ColorMap, reorder} from "../util";
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

type BBox = [[number, number], [number, number]]

export class GeoPlot extends Plot<MapAndColumnRepresentation> {
    protected aggregate: Map<String, number> | null = null;
    protected geoGenerator: GeoPath | null = null;
    public projection: GeoProjection | null = null;
    protected scale: number;
    protected xShift: number;
    protected yShift: number;

    public constructor(surface: PlottingSurface,
                       protected colorMap: ColorMap) {
        super(surface);
        this.scale = 1;
        this.xShift = 0;
        this.yShift = 0;
    }

    public setMap(data: MapAndColumnRepresentation): void {
        this.data = data;
    }

    public setOrientation(scale: number, xShift: number, yShift: number): void {
        this.scale = scale;
        this.xShift = xShift;
        this.yShift = yShift;
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
        const scale = this.projection.scale();
        this.projection.scale(this.scale * scale);
        const [x, y] = this.projection.translate();
        this.projection.translate([this.xShift + x, this.yShift + y]);
        this.geoGenerator = geoPath().projection(this.projection);
        chart.selectAll('path')
            .data(this.data.data.features)
            .enter()
            .append('path')
            .attr('d', this.geoGenerator)
            .attr("fill", (d: Feature<GeometryObject>) => {
                const prop = d.properties![this.data.property];
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
        const radius = 5;
        let toDisplay: Feature<GeometryObject> | null = null;

        for (const f of this.data.data.features) {
            if (f.geometry.type === "Point") {
                const center = this.geoGenerator!.centroid(f);
                if (Math.abs(center[0] - x) < radius &&
                    Math.abs(center[1] - y) < radius) {
                    toDisplay = f;
                    break;
                }
            } else if (geoContains(f, coords)) {
                toDisplay = f;
                break;
            }
        }

        if (toDisplay != null) {
            const bounds = this.geoGenerator!.bounds(toDisplay);
            const prop = toDisplay.properties![this.data.property];
            const value = this.count(prop);
            return {
                bounds,
                property: prop,
                value
            };
        }
        return null;
    }

    static overlap(box0: BBox, box1: BBox): boolean {
        if (box0[0][0] >= box1[1][0] || box1[0][0] >= box0[1][0])
            return false;
        // noinspection RedundantIfStatementJS
        if (box0[0][1] >= box1[1][1] || box1[0][1] >= box0[1][1])
            return false;
        return true;
    }

    static inside(outer: BBox, inner: BBox): boolean {
        return inner[0][0] >= outer[0][0] && inner[1][0] <= outer[1][0] &&
            inner[0][1] >= outer[0][1] && inner[1][1] <= outer[1][1];
    }

    public within(xl: number, xr: number, yl: number, yr: number): string[] {
        const result: string[] = [];
        [xl, xr] = reorder(xl, xr);
        [yl, yr] = reorder(yl, yr);
        for (const f of this.data.data.features) {
            const box = this.geoGenerator!.bounds(f);
            if (GeoPlot.overlap([[xl, yl], [xr, yr]], box))
                result.push(f.properties[this.data.property]);
        }
        return result;
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
