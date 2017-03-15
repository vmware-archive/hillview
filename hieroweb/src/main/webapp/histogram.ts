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

import {IHtmlElement, HieroDataView, FullPage, Renderer, removeAllChildren} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";

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

export class Histogram extends RemoteObject
    implements IHtmlElement, HieroDataView {
    private topLevel: HTMLElement;
    public readonly width: number = 500;
    public readonly height: number = 200;
    private barWidth = 10;
    private topSpace = 20;
    protected page: FullPage;
    protected svg: any;

    constructor(id: string, page: FullPage) {
        super(id);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.setPage(page);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public updateView(h: Histogram1D) : void {
        let data = h.buckets.map(b => b.count);
        let max = d3.max(data);

        removeAllChildren(this.topLevel);

        let svg = d3.select(this.topLevel)
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.height);

        let y = d3.scaleLinear()
            .domain([0, max])
            .range([0, this.height]);

        let barWidth = this.width / data.length;

        let bar = svg.selectAll("g")
            .data(data)
            .enter().append("g")
            .attr("transform", (d, i) => "translate(" + i * barWidth + ",0)");

        bar.append("rect")
            .attr("y", d => this.height - y(d))
            .attr("height", d => y(d))
            .attr("width", barWidth - 1);

        bar.append("text")
            .attr("x", barWidth / 2)
            .attr("y", d => this.height - y(d))
            .attr("dy", d => d <= (max / 2) ? "-.25em" : ".75em")
            .attr("fill", d => d <= (max / 2) ? "black" : "white")
            .text(d => d);
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

export class HistogramRenderer extends Renderer<Histogram1D> {
    protected histogram: Histogram;

    constructor(page: FullPage,
                remoteTableId: string,
                operation: ICancellable) {
        super(page.progressManager.newProgressBar(operation, "histogram"),
            page.getErrorReporter());
        this.histogram = new Histogram(remoteTableId, page);
        page.setHieroDataView(this.histogram);
    }

    onNext(value: PartialResult<Histogram1D>): void {
        this.progressBar.setPosition(value.done);
        this.histogram.updateView(value.data);
    }
}