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
    Groups,
    IColumnDescription, MapAndColumnRepresentation,
    NextKList,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {ChartView} from "./chartView";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, Converters, ICancellable} from "../util";
import {BaseReceiver} from "../tableTarget";
import {DisplayName, SchemaClass} from "../schemaClass";
import {CommonArgs, OnCompleteReceiverCommon, ReceiverCommonArgs} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {IDataView} from "../ui/dataview";
import {IViewSerialization, MapSerialization} from "../datasetView";
import {mouse as d3mouse} from "d3-selection";
import {OnCompleteReceiver, RpcRequest} from "../rpc";
import {GeoPlot} from "../ui/geoPlot";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
import {HeatmapLegendPlot} from "../ui/heatmapLegendPlot";

export class GeoView extends ChartView<Groups<number>> {
    protected readonly viewMenu: SubMenu;
    protected readonly plot: GeoPlot;
    protected readonly legend: HeatmapLegendPlot;
    protected keyColumn: IColumnDescription;

    constructor(args: CommonArgs, page: FullPage) {
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
        const legendSurface = new HtmlPlottingSurface(
            this.legendDiv!, this.page, { height: Resolution.legendSpaceHeight });
        // noinspection JSUnusedLocalSymbols
        this.legend = new HeatmapLegendPlot(legendSurface, (xl, xr) => {});
        this.createDiv("chart");
        const surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {});
        this.plot = new GeoPlot(surface, this.legend.getColorMap());
        this.createDiv("summary");
    }

    public static reconstruct(ser: MapSerialization, page: FullPage): IDataView | null {
        const args = this.validateSerialization(ser);
        if (args == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        if (schema == null)
            return null;
        return new GeoView(args, page);
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result = {
            // TODO
            ...super.serialize()
        };
        return result;
    }

    protected export(): void {
        // TODO
    }

    protected getCombineRenderer(title: PageTitle): (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        assert(false);
    }

    protected onMouseMove(): void {
        // TODO
    }

    refresh(): void {
        // TODO
    }

    resize(): void {
        // TODO
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

    public setMap(v: MapAndColumnRepresentation, ketColumn: IColumnDescription): void {
        this.keyColumn = ketColumn;
        this.plot.setMap(v);
        assert(this.summary != null);
        this.summary.set("Polygons", v.data.features.length);
        this.summary.display();
    }

    public updateView(n: NextKList): void {
        const map = new Map<String, number>();
        let max = 0;
        for (const r of n.rows) {
            const count = r.count;
            const value = Converters.valueToString(r.values[0], this.keyColumn.kind, false);
            map.set(value, count);
            if (count > max)
                max = count;
        }

        this.plot.setData(map);
        this.legend.setData(max);
        this.legend.draw();
        this.plot.draw();
    }
}

export class GeoMapReceiver extends OnCompleteReceiverCommon<MapAndColumnRepresentation> {
    protected geoView: GeoView;

    constructor(args: ReceiverCommonArgs, protected keyColumn: IColumnDescription, request: RpcRequest<MapAndColumnRepresentation>) {
        super(args, request, "map");
        this.geoView = new GeoView(args, this.page);
    }

    public run(v: MapAndColumnRepresentation): void {
        this.geoView.setMap(v, this.keyColumn);
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

export class GeoDataReceiver extends OnCompleteReceiver<NextKList> {
    constructor(protected geoView: GeoView, request: RpcRequest<NextKList>) {
        super(geoView.page, request, "map");
    }

    public run(v: NextKList): void {
        this.geoView.updateView(v);
    }
}