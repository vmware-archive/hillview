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

import {Groups, RemoteObjectId, SimpleFeatureCollection} from "../javaBridge";
import {ChartView} from "./chartView";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, ICancellable} from "../util";
import {BaseReceiver} from "../tableTarget";
import {DisplayName, SchemaClass} from "../schemaClass";
import {CommonArgs, OnCompleteReceiverCommon, ReceiverCommonArgs} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {IDataView} from "../ui/dataview";
import {IViewSerialization, MapSerialization} from "../datasetView";
import {mouse as d3mouse} from "d3-selection";
import {RpcRequest} from "../rpc";
import {GeoPlot} from "../ui/geoPlot";
import {HtmlPlottingSurface} from "../ui/plottingSurface";

export class GeoView extends ChartView<Groups<number>> {
    protected readonly viewMenu: SubMenu;
    protected readonly plot: GeoPlot;

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
        this.createDiv("chart");
        const surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        this.plot = new GeoPlot(surface, null);
        this.createDiv("summary");
    }

    public static reconstruct(ser: MapSerialization, page: FullPage): IDataView | null {
        const args = this.validateSerialization(ser);
        if (args == null)
            return null;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
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
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
        return true;
    }

    private selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        // TODO
    }

    public updateView(v: SimpleFeatureCollection): void {
        this.plot.setData(v);
        this.plot.draw();
        this.summary.set("Polygons", v.features.length);
        this.summary.display();
    }
}

export class GeoReceiver extends OnCompleteReceiverCommon<SimpleFeatureCollection> {
    protected geoView: GeoView;

    constructor(args: ReceiverCommonArgs, request: RpcRequest<SimpleFeatureCollection>) {
        super(args, request, "map");
        this.geoView = new GeoView(args, this.page);
    }

    public run(v: SimpleFeatureCollection): void {
        this.geoView.updateView(v);
    }
}