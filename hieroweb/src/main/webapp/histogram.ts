/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {
    IHtmlElement, HieroDataView, FullPage, Renderer, removeAllChildren, significantDigits,
    getWindowSize
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";
import {ColumnDescription, TableView} from "./table";
import {histogram} from "d3-array";

// same as a Java class
interface Bucket1D {
    minObject: any;
    maxObject: any;
    minValue:  number;
    maxValue:  number;
    count:     number;
}

// same as a Java class
interface Histogram1D {
    missingData: number;
    outOfRange:  number;
    buckets:     Bucket1D[];
}

// same as Java class
interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    minObject: any;
    maxObject: any;
    moments: Array<number>;
    rowCount: number;
}

export class Histogram extends RemoteObject
    implements IHtmlElement, HieroDataView {
    private topLevel: HTMLElement;
    public readonly margin = {
        top: 30,
        right: 30,
        bottom: 30,
        left: 40
    };
    private barWidth = 10;
    private topSpace = 20;
    protected page: FullPage;
    protected svg: any;
    private maxHeight = 200;
    protected currentData: {
        histogram: Histogram1D,
        description: ColumnDescription,
        stats: BasicColStats
    };

    constructor(id: string, page: FullPage) {
        super(id);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.setPage(page);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    static translateString(x: number, y: number): string {
        return "translate(" + String(x) + ", " + String(y) + ")";
    }

    public refresh(): void {
        this.updateView(this.currentData.histogram,
            this.currentData.description,
            this.currentData.stats);
    }

    public updateView(h: Histogram1D, cd: ColumnDescription, stats: BasicColStats) : void {
        this.currentData = { histogram: h, description: cd, stats: stats };

        let ws = getWindowSize();
        let width = ws.width;
        let height = ws.height;
        if (height > this.maxHeight)
            height = this.maxHeight;

        let chartWidth = width - this.margin.left - this.margin.right;
        let chartHeight = height - this.margin.top - this.margin.bottom;

        let counts = h.buckets.map(b => b.count);
        let max = d3.max(counts);
        let min = d3.min(counts);
        removeAllChildren(this.topLevel);

        let canvas = d3.select(this.topLevel)
            .append("svg")
            .attr("width", width)
            .attr("height", height);

        let chart = canvas
            .append("g")
            .attr("transform", Histogram.translateString(this.margin.left, this.margin.top));

        let y = d3.scaleLinear()
            .domain([0, max])
            .range([chartHeight, 0]);
        let yAxis = d3.axisLeft(y);

        let x = d3.scaleLinear()
            .domain([stats.min, stats.max])
            .range([0, chartWidth]);
        let xAxis = d3.axisBottom(x);

        canvas.append("text")
            .text(cd.name)
            .attr("transform", Histogram.translateString(chartWidth / 2, this.margin.top/2))
            .attr("text-anchor", "middle");

        let barWidth = chartWidth / counts.length;
        let bars = chart.selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => Histogram.translateString(i * barWidth, 0));

        bars.append("rect")
            .attr("y", d => y(d))
            .attr("height", d => chartHeight - y(d))
            .attr("width", barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", d => y(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (max / 2) ? "-.25em" : ".75em")
            .attr("fill", d => d <= (max / 2) ? "black" : "white")
            .text(d => (d == 0) ? "" : significantDigits(d))
            .exit();

        chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", Histogram.translateString(0, chartHeight))
            .call(xAxis);

        console.log(String(counts.length) + " data points");

        let infoBox = document.createElement("div");
        this.topLevel.appendChild(infoBox);
        if (h.missingData != 0)
            infoBox.textContent = String(h.missingData) + " missing, ";
        infoBox.textContent += String(stats.rowCount) + " points";
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }
}

// Waits for all column stats to be received and then initiates a histogram
// rendering.
export class RangeCollector extends Renderer<BasicColStats> {
    protected stats: BasicColStats;

    constructor(protected cd: ColumnDescription,
                page: FullPage,
                protected table: TableView,
                operation: ICancellable) {
        super(page, operation, "histogram");
    }

    onNext(value: PartialResult<BasicColStats>): void {
        this.progressBar.setPosition(value.done);
        this.stats = value.data;
    }

    onCompleted(): void {
        super.onCompleted();
        let rr = this.table.createRpcRequest("histogram", {
            columnName: this.cd.name,
            min: this.stats.min,
            max: this.stats.max
        });
        let renderer = new HistogramRenderer(
            this.page, this.table.remoteObjectId, this.cd, this.stats, rr);
        rr.invoke(renderer);
    }
}

// Renders a column histogram
export class HistogramRenderer extends Renderer<Histogram1D> {
    protected histogram: Histogram;

    constructor(page: FullPage,
                remoteTableId: string,
                protected cd: ColumnDescription,
                protected stats: BasicColStats,
                operation: ICancellable) {
        super(page, operation, "histogram");
        this.histogram = new Histogram(remoteTableId, page);
        page.setHieroDataView(this.histogram);
    }

    onNext(value: PartialResult<Histogram1D>): void {
        this.progressBar.setPosition(value.done);
        this.histogram.updateView(value.data, this.cd, this.stats);
    }
}