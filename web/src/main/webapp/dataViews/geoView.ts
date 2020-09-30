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

import {
    FilterListDescription,
    IColumnDescription, MapAndColumnRepresentation,
    NextKList,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {ChartView} from "./chartView";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, Converters, Exporter, ICancellable, PartialResult, significantDigits} from "../util";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {CommonArgs, TableMeta, OnCompleteReceiverCommon, ReceiverCommonArgs} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {IDataView} from "../ui/dataview";
import {IViewSerialization, MapSerialization} from "../datasetView";
import {mouse as d3mouse} from "d3-selection";
import {Receiver, RpcRequest} from "../rpc";
import {GeoPlot} from "../ui/geoPlot";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {Resolution, SpecialChars} from "../ui/ui";
import {HeatmapLegendPlot} from "../ui/heatmapLegendPlot";
import {TextOverlay} from "../ui/textOverlay";
import {saveAs} from "../ui/dialog";

export class GeoView extends ChartView<NextKList> {
    protected readonly viewMenu: SubMenu;
    protected plot: GeoPlot | null = null;
    protected legend: HeatmapLegendPlot | null = null;
    protected legendSurface: HtmlPlottingSurface | null = null;
    protected pointDescription: TextOverlay | null = null;
    protected mapData: MapAndColumnRepresentation | null = null;
    protected defaultProvenance: string = "Map view";
    protected navigation: HTMLDivElement;
    protected scale: number = 1;  // magnification of map
    protected xShift: number = 0; // translation of map
    protected yShift: number = 0;

    constructor(args: CommonArgs, protected readonly keyColumn: IColumnDescription, page: FullPage) {
        super(args.remoteObject.remoteObjectId, args, page, "Map");
        const zoomIncrement = 1.3;
        this.viewMenu = new SubMenu([{
                text: "refresh",
                action: () => this.refresh(),
                help: "Redraw this view.",
            }, {
                text: "table",
                action: () => this.showTable([this.keyColumn], this.defaultProvenance),
                help: "Show the data underlying this map using a table view.",
            }]);
        this.menu = new TopMenu([
            this.exportMenu(),
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);
        this.page.setMenu(this.menu);
        this.createDiv("legend");
        const n = this.makeToplevelDiv("navigationBar");
        n.style.textAlign = "center";
        this.navigation = document.createElement("div");
        n.append(this.navigation);
        this.createDiv("chart");
        this.createDiv("summary");

        this.createNavigation(
            "o", "Center map",
            () => { this.scale = 1; this.xShift = 0; this.yShift = 0; this.resize(); });
        this.createNavigation("+", "Zoom in", () => this.zoom(zoomIncrement));
        const shiftAmount = 50;
        this.createNavigation(SpecialChars.enDash, "Zoom out", () => this.zoom(1 / zoomIncrement));
        this.createNavigation(SpecialChars.leftArrowHtml, "Move left", () => this.translate(shiftAmount, 0));
        this.createNavigation(SpecialChars.downArrowHtml, "Move down", () => this.translate(0, shiftAmount));
        this.createNavigation(SpecialChars.rightArrowHtml, "Move right", () => this.translate(-shiftAmount, 0));
        this.createNavigation(SpecialChars.upArrowHtml, "Move up", () => this.translate(0, -shiftAmount));
    }

    protected createNavigation(html: string, title: string, method: () => void): void {
        const button = document.createElement("button");
        button.innerHTML = html;
        button.onclick = method;
        button.title = title;
        button.className = "navigation";
        this.navigation.appendChild(button);
    }

    public static reconstruct(ser: MapSerialization, page: FullPage): IDataView | null {
        const args = this.validateSerialization(ser);
        if (args == null || ser.keyColumn == null)
            return null;
        const cd = args.schema.find(ser.keyColumn);
        if (cd == null)
            return null;
        return new GeoView(args, cd, page);
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: MapSerialization = {
            ...super.serialize(),
            keyColumn: this.keyColumn.name
        };
        return result;
    }

    public translate(left: number, up: number): void {
        this.xShift -= left;
        this.yShift += up;
        this.resize();
    }

    protected export(): void {
        const order = new RecordOrder([
            { columnDescription: this.keyColumn, isAscending: true}]);
        const lines = Exporter.tableAsCsv(order, this.meta.schema, null, this.data);
        const fileName = "map.csv";
        saveAs(fileName, lines.join("\n"));
    }

    protected getCombineRenderer(title: PageTitle): (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        assert(false);
    }

    protected onMouseMove(): void {
        if (this.plot == null)
            return;
        const position = d3mouse(this.surface!.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];
        const box = this.plot.get(mouseX, mouseY);
        if (box == null) {
            this.pointDescription!.show(false);
            return;
        }
        this.pointDescription!.show(true);
        const v = ((box.value != null) ? " " + significantDigits(box.value) : "");
        this.pointDescription!.update([box.property, v], mouseX, mouseY);
    }

    refresh(): void {
        const rr = this.createGeoRequest(this.keyColumn);
        const args: ReceiverCommonArgs = {
            title: new PageTitle("Count of " + this.keyColumn.name,
                this.defaultProvenance),
            remoteObject: this,
            originalPage: this.page,
            options: { chartKind: "Map", reusePage: false },
            ...this.meta,
        };
        const rec = new GeoMapReceiver(args, this.keyColumn, rr, this);
        rr.invoke(rec);
    }

    resize(): void {
        if (this.data == null)
            return;
        this.updateView(this.data, true);
    }

    protected showTrellis(colName: string): void {
        // TODO
    }

    protected dragStart(): void {
        this.dragStartRectangle();
    }

    protected dragMove(): boolean {
        return this.dragMoveRectangle();
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd() || this.selectionOrigin == null)
            return false;
        const position = d3mouse(this.surface!.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
        return true;
    }

    private selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        if (this.plot == null)
            return;
        const keep = this.plot.within(xl, xr, yl, yr);
        if (keep.length == 0) {
            this.page.reportError("No objects overlap selection");
            return;
        }
        const filter: FilterListDescription = {
            column: this.keyColumn.name,
            keep
        }
        const rr = this.createFilterListRequest(filter);
        rr.invoke(new FilterMapReceiver(this.page, this.keyColumn, this.meta, rr));
    }

    public setMap(mapData: MapAndColumnRepresentation, keepColorMap: boolean): void {
        this.createNewSurfaces(keepColorMap);
        this.mapData = mapData;
        this.plot!.setMap(mapData);
    }

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.legendSurface = new HtmlPlottingSurface(
            this.legendDiv!, this.page, { height: Resolution.legendSpaceHeight });
        // noinspection JSUnusedLocalSymbols
        if (!keepColorMap) {
            this.legend = new HeatmapLegendPlot(this.legendSurface,
                (xl, xr) => this.legendSelectionCompleted(xl, xr));
            this.legend!.setColorMapChangeEventListener(() => this.updateView(this.data, true));
        } else {
            this.legend!.moved();
            this.legend!.setSurface(this.legendSurface);
        }
        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {});
        this.plot = new GeoPlot(this.surface, this.legend!.getColorMap());
        this.setupMouse();
    }

    protected zoom(scale: number): void {
        this.scale *= scale;
        const scaleLimit = 50;
        if (this.scale > scaleLimit)
            this.scale = scaleLimit;
        if (this.scale < 1/scaleLimit)
            this.scale = 1/scaleLimit;
        this.resize();
    }

    protected legendSelectionCompleted(xl: number, xr: number): void {
        this.legend!.emphasizeRange(xl, xr);
    }

    public draw(): void {
        this.plot!.setOrientation(this.scale, this.xShift, this.yShift);
        this.legend!.draw();
        this.plot!.draw();
        // Create point description last so it is shown on top
        assert(this.surface != null);
        const pointDesc = [this.keyColumn.name, "count"];
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(), pointDesc, 40);
        assert(this.summary != null);
        this.summary.set("Polygons", this.mapData!.data.features.length);
        this.summary.display();
    }

    public updateView(n: NextKList, keepColorMap: boolean): void {
        this.data = n;
        if (n == null)
            return;
        // Creates new surfaces.
        this.setMap(this.mapData!, keepColorMap);
        const map = new Map<String, number>();
        let max = 0;
        for (const r of n.rows) {
            const count = r.count;
            const value = Converters.valueToString(r.values[0], this.keyColumn.kind, false);
            map.set(value, count);
            if (count > max)
                max = count;
        }
        if (!keepColorMap)
            this.legend!.setData(max);
        this.plot!.setData(map);
        this.summary!.set("Objects", n.rows.length);
        this.draw();
    }
}

export class GeoMapReceiver extends OnCompleteReceiverCommon<MapAndColumnRepresentation> {
    protected geoView: GeoView;

    constructor(readonly args: ReceiverCommonArgs, readonly keyColumn: IColumnDescription,
                readonly request: RpcRequest<MapAndColumnRepresentation>, view: GeoView | null) {
        super(args, request, "map");
        if (view == null)
            this.geoView = new GeoView(args, keyColumn, this.page);
        else
            this.geoView = view;
    }

    public run(v: MapAndColumnRepresentation): void {
        this.geoView.setMap(v, false);
        this.geoView.draw();
        const ro = new RecordOrder([{
            columnDescription: this.keyColumn,
            isAscending: true
        }]);
        // TODO: this is not correct, since the values in the column and the features
        // in the dataset may not be exactly the same.  We use a 2x here, but
        // the right thing is to use hyperLogLog to estimate the count.
        const rr = this.args.remoteObject.createNextKRequest(
            ro, null, 2 * v.data.features.length, null, null);
        rr.invoke(new GeoDataReceiver(this.geoView, rr));
    }
}

export class GeoDataReceiver extends Receiver<NextKList> {
    constructor(protected geoView: GeoView, request: RpcRequest<NextKList>) {
        super(geoView.page, request, "map");
    }

    public onNext(v: PartialResult<NextKList>): void {
        super.onNext(v);
        if (v == null || v.data == null)
            return;
        this.geoView.updateView(v.data, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.geoView.updateCompleted(this.elapsedMilliseconds());
    }
}

export class FilterMapReceiver extends BaseReceiver {
    constructor(page: FullPage,
                protected keyColumn: IColumnDescription,
                protected meta: TableMeta,
                operation: ICancellable<RemoteObjectId>) {
        super(page, operation, "Filter", page.dataset);
    }

    public run(value: RemoteObjectId): void {
        super.run(value); // This sets this.remoteObject.
        const remoteObject = new TableTargetAPI(value);
        const rr = remoteObject.createGeoRequest(this.keyColumn);
        const args: ReceiverCommonArgs = {
            title: this.page.title,
            remoteObject: remoteObject,
            ...this.meta,
            originalPage: this.page,
            options: { chartKind: "Map", reusePage: false }
        };
        const rec = new GeoMapReceiver(args, this.keyColumn, rr, null);
        rr.invoke(rec);
    }
}
