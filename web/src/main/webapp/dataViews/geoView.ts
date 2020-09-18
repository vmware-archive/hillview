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
    IColumnDescription, MapAndColumnRepresentation,
    NextKList,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {ChartView} from "./chartView";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, Converters, Exporter, ICancellable, PartialResult, significantDigits} from "../util";
import {BaseReceiver} from "../tableTarget";
import {DisplayName} from "../schemaClass";
import {CommonArgs, OnCompleteReceiverCommon, ReceiverCommonArgs} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {IDataView} from "../ui/dataview";
import {IViewSerialization, MapSerialization} from "../datasetView";
import {mouse as d3mouse} from "d3-selection";
import {Receiver, RpcRequest} from "../rpc";
import {GeoPlot} from "../ui/geoPlot";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
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

    constructor(args: CommonArgs, protected readonly keyColumn: IColumnDescription, page: FullPage) {
        super(args.remoteObject.remoteObjectId, args.rowCount, args.schema, page, "Map");
        this.viewMenu = new SubMenu([
            {
                text: "refresh",
                action: () => {
                    this.refresh();
                },
                help: "Redraw this view.",
            }]);
        this.menu = new TopMenu([
            this.exportMenu(),
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);
        this.page.setMenu(this.menu);
        this.createDiv("legend");
        this.createDiv("chart");
        this.createDiv("summary");
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

    protected export(): void {
        const order = new RecordOrder([
            { columnDescription: this.keyColumn, isAscending: true}]);
        const lines = Exporter.tableAsCsv(order, this.schema, null, this.data);
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
            title: new PageTitle("Count of " + this.schema.displayName(this.keyColumn.name)!.displayName,
                this.defaultProvenance),
            remoteObject: this,
            rowCount: this.rowCount,
            schema: this.schema,
            originalPage: this.page,
            options: { chartKind: "Map", reusePage: false }
        };
        const rec = new GeoMapReceiver(args, this.keyColumn, rr);
        rr.invoke(rec);
    }

    resize(): void {
        if (this.data == null)
            return;
        this.updateView(this.data);
    }

    protected showTrellis(colName: DisplayName): void {
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
        // TODO
    }

    public setMap(mapData: MapAndColumnRepresentation): void {
        this.createNewSurfaces();
        this.mapData = mapData;
        this.plot!.setMap(mapData);
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.legendSurface = new HtmlPlottingSurface(
            this.legendDiv!, this.page, { height: Resolution.legendSpaceHeight });
        // noinspection JSUnusedLocalSymbols
        this.legend = new HeatmapLegendPlot(this.legendSurface, (xl, xr) => {});
        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {});
        this.plot = new GeoPlot(this.surface, this.legend.getColorMap());
        this.setupMouse();
    }

    public draw(): void {
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

    public updateView(n: NextKList): void {
        this.data = n;
        if (n == null)
            return;
        this.setMap(this.mapData!);
        const map = new Map<String, number>();
        let max = 0;
        for (const r of n.rows) {
            const count = r.count;
            const value = Converters.valueToString(r.values[0], this.keyColumn.kind, false);
            map.set(value, count);
            if (count > max)
                max = count;
        }
        this.legend!.setData(max);
        this.plot!.setData(map);
        this.draw();
    }
}

export class GeoMapReceiver extends OnCompleteReceiverCommon<MapAndColumnRepresentation> {
    protected geoView: GeoView;

    constructor(readonly args: ReceiverCommonArgs, readonly keyColumn: IColumnDescription, readonly request: RpcRequest<MapAndColumnRepresentation>) {
        super(args, request, "map");
        this.geoView = new GeoView(args, keyColumn, this.page);
    }

    public run(v: MapAndColumnRepresentation): void {
        this.geoView.setMap(v);
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
        this.geoView.updateView(v.data);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.geoView.updateCompleted(this.elapsedMilliseconds());
    }
}
