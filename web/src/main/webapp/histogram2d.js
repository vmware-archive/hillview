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
"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var histogramBase_1 = require("./histogramBase");
var table_1 = require("./table");
var ui_1 = require("./ui");
var menu_1 = require("./menu");
var d3 = require('d3');
var util_1 = require("./util");
var heatMap_1 = require("./heatMap");
var Histogram2DView = (function (_super) {
    __extends(Histogram2DView, _super);
    function Histogram2DView(remoteObjectId, tableSchema, page) {
        var _this = this;
        _super.call(this, remoteObjectId, tableSchema, page);
        this.tableSchema = tableSchema;
        var menu = new menu_1.DropDownMenu([
            { text: "View", subMenu: new menu_1.ContextMenu([
                    { text: "refresh", action: function () { _this.refresh(); } },
                    { text: "table", action: function () { return _this.showTable(); } },
                    { text: "percent/value", action: function () { _this.normalized = !_this.normalized; _this.refresh(); } },
                ]) }
        ]);
        this.normalized = false;
        this.topLevel.insertBefore(menu.getHTMLRepresentation(), this.topLevel.children[0]);
    }
    Histogram2DView.prototype.refresh = function () {
        if (this.currentData == null)
            return;
        this.updateView(this.currentData.data, this.currentData.xData, this.currentData.yData, this.currentData.missingData, 0);
    };
    Histogram2DView.prototype.onMouseMove = function () {
        var position = d3.mouse(this.chart.node());
        var mouseX = position[0];
        var mouseY = position[1];
        var x = 0;
        if (this.xScale != null)
            x = this.xScale.invert(position[0]);
        if (this.currentData.xData.description.kind == "Integer")
            x = Math.round(x);
        var xs = String(x);
        if (this.currentData.xData.description.kind == "Category") {
            var index = Math.round(x);
            if (index >= 0 && index < this.currentData.xData.allStrings.length)
                xs = this.currentData.xData.allStrings[index];
            else
                xs = "";
        }
        else if (this.currentData.xData.description.kind == "Integer" ||
            this.currentData.xData.description.kind == "Double")
            xs = ui_1.significantDigits(x);
        var y = Math.round(this.yScale.invert(position[1]));
        var ys = ui_1.significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;
        this.xDot.attr("cx", mouseX + histogramBase_1.HistogramViewBase.margin.left);
        this.yDot.attr("cy", mouseY + histogramBase_1.HistogramViewBase.margin.top);
        /*
        if (this.currentData.cdfSum != null) {
            // determine mouse position on cdf curve
            // we have to take into account the adjustment
            let cdfX = (mouseX - this.adjustment / 2) * this.currentData.cdfSum.length /
                (this.chartResolution.width - this.adjustment);
            let pos = 0;
            if (cdfX < 0) {
                pos = 0;
            } else if (cdfX >= this.currentData.cdfSum.length) {
                pos = 1;
            } else {
                let cdfPosition = this.currentData.cdfSum[Math.floor(cdfX)];
                pos = cdfPosition / this.currentData.stats.presentCount;
            }

            this.cdfDot.attr("cx", mouseX + HistogramViewBase.margin.left);
            this.cdfDot.attr("cy", (1 - pos) * this.chartResolution.height + HistogramViewBase.margin.top);
            let perc = percent(pos);
            this.cdfLabel.textContent = "cdf=" + perc;
        }
        */
    };
    Histogram2DView.prototype.updateView = function (data, xData, yData, missingData, elapsedMs) {
        var _this = this;
        this.page.reportError("Operation took " + ui_1.significantDigits(elapsedMs / 1000) + " seconds");
        if (data == null || data.length == 0) {
            this.page.reportError("No data to display");
            return;
        }
        var xPoints = data.length;
        var yRectangles = data[0].length;
        if (yRectangles == 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            data: data,
            xData: xData,
            yData: yData,
            missingData: missingData,
            xPoints: xPoints,
            yPoints: yRectangles
        };
        var width = this.page.getWidthInPixels();
        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        var chartWidth = width - histogramBase_1.HistogramViewBase.margin.left - histogramBase_1.HistogramViewBase.margin.right;
        if (chartWidth < histogramBase_1.HistogramViewBase.minChartWidth)
            chartWidth = histogramBase_1.HistogramViewBase.minChartWidth;
        var chartHeight = histogramBase_1.HistogramViewBase.chartHeight;
        var canvasHeight = chartHeight + histogramBase_1.HistogramViewBase.margin.top + histogramBase_1.HistogramViewBase.margin.bottom;
        this.chartResolution = { width: chartWidth, height: histogramBase_1.HistogramViewBase.chartHeight };
        /*
        let counts = h.buckets;
        let bucketCount = counts.length;
        let max = d3.max(counts);

        // prefix sum for cdf
        let cdfData: number[] = [];
        if (cdf != null) {
            this.currentData.cdfSum = [];

            let sum = 0;
            for (let i in cdf.buckets) {
                sum += cdf.buckets[i];
                this.currentData.cdfSum.push(sum);
            }

            let point = 0;
            for (let i in this.currentData.cdfSum) {
                cdfData.push(point);
                point = this.currentData.cdfSum[i] * max / stats.presentCount;
                cdfData.push(point);
            }
        }
        */
        if (this.canvas != null)
            this.canvas.remove();
        var counts = [];
        var max = 0;
        var rects = [];
        var total = 0;
        for (var x = 0; x < data.length; x++) {
            var yTotal = 0;
            for (var y = 0; y < data[x].length; y++) {
                var v = data[x][y];
                total += v;
                if (v != 0) {
                    var rec = {
                        x: x,
                        y: yTotal,
                        index: y,
                        height: v
                    };
                    rects.push(rec);
                }
                yTotal += v;
            }
            if (yTotal > max)
                max = yTotal;
            counts.push(yTotal);
        }
        if (max <= 0) {
            this.page.reportError("No data");
            return;
        }
        var drag = d3.drag()
            .on("start", function () { return _this.dragStart(); })
            .on("drag", function () { return _this.dragMove(); })
            .on("end", function () { return _this.dragEnd(); });
        this.canvas = d3.select(this.chartDiv)
            .append("svg")
            .attr("id", "canvas")
            .call(drag)
            .attr("width", width)
            .attr("border", 1)
            .attr("height", canvasHeight)
            .attr("cursor", "crosshair");
        this.canvas.on("mousemove", function () { return _this.onMouseMove(); });
        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", ui_1.translateString(histogramBase_1.HistogramViewBase.margin.left, histogramBase_1.HistogramViewBase.margin.top));
        this.yScale = d3.scaleLinear()
            .range([histogramBase_1.HistogramViewBase.chartHeight, 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, max]);
        var yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));
        var cd = xData.description;
        var bucketCount = xPoints;
        var minRange = xData.stats.min;
        var maxRange = xData.stats.max;
        this.adjustment = 0;
        if (cd.kind == "Integer" || cd.kind == "Category" || xData.stats.min >= xData.stats.max) {
            minRange -= .5;
            maxRange += .5;
            this.adjustment = chartWidth / (maxRange - minRange);
        }
        var xAxis = null;
        this.xScale = null;
        if (cd.kind == "Integer" ||
            cd.kind == "Double") {
            this.xScale = d3.scaleLinear()
                .domain([minRange, maxRange])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(this.xScale);
        }
        else if (cd.kind == "Category") {
            var ticks = [];
            var labels = [];
            for (var i = 0; i < bucketCount; i++) {
                var index = i * (maxRange - minRange) / bucketCount;
                index = Math.round(index);
                ticks.push(this.adjustment / 2 + index * chartWidth / (maxRange - minRange));
                labels.push(this.currentData.xData.allStrings[xData.stats.min + index]);
            }
            var axisScale = d3.scaleOrdinal()
                .domain(labels)
                .range(ticks);
            this.xScale = d3.scaleLinear()
                .domain([minRange, maxRange])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(axisScale);
        }
        else if (cd.kind == "Date") {
            var minDate = util_1.Converters.dateFromDouble(minRange);
            var maxDate = util_1.Converters.dateFromDouble(maxRange);
            this.xScale = d3
                .scaleTime()
                .domain([minDate, maxDate])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(this.xScale);
        }
        // force a tick on x axis for degenerate scales
        if (xData.stats.min >= xData.stats.max && xAxis != null)
            xAxis.ticks(1);
        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", ui_1.translateString(chartWidth / 2, chartHeight +
            histogramBase_1.HistogramViewBase.margin.top + histogramBase_1.HistogramViewBase.margin.bottom / 2))
            .attr("text-anchor", "middle");
        this.canvas.append("text")
            .text(yData.description.name)
            .attr("transform", ui_1.translateString(chartWidth / 2, 0))
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");
        /*
        // After resizing the line may not have the exact number of points
        // as the screen width.
        let cdfLine = d3.line<number>()
            .x((d, i) => {
                let index = Math.floor(i / 2); // two points for each data point, for a zig-zag
                return this.adjustment/2 + index * 2 * (chartWidth - this.adjustment) / cdfData.length;
            })
            .y(d => this.yScale(d));

        // draw CDF curve
        this.canvas.append("path")
            .attr("transform", translateString(
                HistogramViewBase.margin.left, HistogramViewBase.margin.top))
            .datum(cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");
            */
        var barWidth = chartWidth / bucketCount;
        var scale = chartHeight / max;
        var bars = this.chart.selectAll("g")
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", function (d) { return d.x * barWidth; })
            .attr("y", function (d) { return _this.rectPosition(d, counts, scale); })
            .attr("height", function (d) { return _this.rectHeight(d, counts, scale); })
            .attr("width", barWidth - 1)
            .attr("fill", function (d) { return _this.color(d.index, yRectangles - 1); })
            .exit();
        this.chart.selectAll("g")
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", function (d) { return d * scale; })
            .attr("text-anchor", "middle")
            .attr("dy", function (d) { return d <= (9 * max / 10) ? "-.25em" : ".75em"; })
            .text(function (d) { return (d == 0) ? "" : ui_1.significantDigits(d); })
            .exit();
        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        if (xAxis != null) {
            this.chart.append("g")
                .attr("class", "x-axis")
                .attr("transform", ui_1.translateString(0, histogramBase_1.HistogramViewBase.chartHeight))
                .call(xAxis);
        }
        var dotRadius = 3;
        this.xDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", histogramBase_1.HistogramViewBase.chartHeight + histogramBase_1.HistogramViewBase.margin.top)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", histogramBase_1.HistogramViewBase.margin.left)
            .attr("cy", 0)
            .attr("fill", "blue");
        this.cdfDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("fill", "blue");
        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);
        var legendWidth = 500;
        if (legendWidth > chartWidth)
            legendWidth = chartWidth;
        var legendHeight = 15;
        var legendSvg = this.canvas
            .append("svg");
        var gradient = legendSvg.append('defs')
            .append('linearGradient')
            .attr('id', 'gradient')
            .attr('x1', '0%')
            .attr('y1', '0%')
            .attr('x2', '100%')
            .attr('y2', '0%')
            .attr('spreadMethod', 'pad');
        for (var i = 0; i <= 100; i += 4) {
            gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", Histogram2DView.colorMap(i / 100))
                .attr("stop-opacity", 1);
        }
        legendSvg.append("rect")
            .attr("width", legendWidth)
            .attr("height", legendHeight)
            .style("fill", "url(#gradient)")
            .attr("x", (chartWidth - legendWidth) / 2)
            .attr("y", histogramBase_1.HistogramViewBase.margin.top / 3);
        // create a scale and axis for the legend
        var legendScale = d3.scaleLinear();
        legendScale
            .domain([1, max])
            .range([0, legendWidth]);
        var legendAxis = d3.axisBottom(legendScale);
        legendSvg.append("g")
            .attr("transform", ui_1.translateString((chartWidth - legendWidth) / 2, legendHeight + histogramBase_1.HistogramViewBase.margin.top / 3))
            .call(legendAxis);
        var summary = ui_1.formatNumber(total) + " data points";
        if (missingData != 0)
            summary += ", " + ui_1.formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + ui_1.formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + ui_1.formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        this.summary.textContent = summary;
    };
    Histogram2DView.prototype.rectHeight = function (d, counts, scale) {
        if (this.normalized) {
            var c = counts[d.x];
            if (c <= 0)
                return 0;
            return histogramBase_1.HistogramViewBase.chartHeight * d.height / c;
        }
        return d.height * scale;
    };
    Histogram2DView.prototype.rectPosition = function (d, counts, scale) {
        var y = d.y + d.height;
        if (this.normalized) {
            var c = counts[d.x];
            if (c <= 0)
                return 0;
            return histogramBase_1.HistogramViewBase.chartHeight * (1 - y / c);
        }
        return histogramBase_1.HistogramViewBase.chartHeight - y * scale;
    };
    Histogram2DView.colorMap = function (d) {
        return d3.sche(d);
    };
    Histogram2DView.prototype.color = function (d, max) {
        return Histogram2DView.colorMap(d / max);
    };
    // show the table corresponding to the data in the histogram
    Histogram2DView.prototype.showTable = function () {
        var table = new table_1.TableView(this.remoteObjectId, this.page);
        table.setSchema(this.tableSchema);
        var order = new table_1.RecordOrder([{
                columnDescription: this.currentData.xData.description,
                isAscending: true
            }, {
                columnDescription: this.currentData.yData.description,
                isAscending: true
            }]);
        var rr = table.createNextKRequest(order, null);
        var page = new ui_1.FullPage();
        page.setHillviewDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new table_1.TableRenderer(page, table, rr, false, order));
    };
    Histogram2DView.prototype.selectionCompleted = function (xl, xr) {
        if (this.xScale == null)
            return;
        var kind = this.currentData.xData.description.kind;
        var x0 = histogramBase_1.HistogramViewBase.invertToNumber(xl, this.xScale, kind);
        var x1 = histogramBase_1.HistogramViewBase.invertToNumber(xr, this.xScale, kind);
        // selection could be done in reverse
        var min;
        var max;
        _a = util_1.reorder(x0, x1), min = _a[0], max = _a[1];
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }
        var boundaries = null;
        if (this.currentData.xData.allStrings != null) {
            // it's enough to just send the first and last element for filtering.
            boundaries = [this.currentData.xData.allStrings[Math.ceil(min)],
                this.currentData.xData.allStrings[Math.floor(max)]];
        }
        var range = {
            min: min,
            max: max,
            columnName: this.currentData.xData.description.name,
            cdfBucketCount: null,
            bucketCount: null,
            bucketBoundaries: boundaries
        };
        var _a;
        /*
        let rr = this.createRpcRequest("filterRange", range);
        let renderer = new FilterReceiver(
            this.currentData.xData.description, this.tableSchema,
            this.currentData.xData.allStrings, range, this.page, rr);
        rr.invoke(renderer);
        */
    };
    return Histogram2DView;
}(histogramBase_1.HistogramViewBase));
exports.Histogram2DView = Histogram2DView;
var Histogram2DRenderer = (function (_super) {
    __extends(Histogram2DRenderer, _super);
    function Histogram2DRenderer(page, remoteTableId, schema, cds, stats, operation) {
        _super.call(this, new ui_1.FullPage(), operation, "histogram");
        this.schema = schema;
        this.cds = cds;
        this.stats = stats;
        page.insertAfterMe(this.page);
        this.histogram = new Histogram2DView(remoteTableId, schema, this.page);
        this.page.setHillviewDataView(this.histogram);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }
    Histogram2DRenderer.prototype.onNext = function (value) {
        _super.prototype.onNext.call(this, value);
        if (value == null)
            return;
        var xAxisData = new heatMap_1.AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], null);
        var yAxisData = new heatMap_1.AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], null);
        this.histogram.updateView(value.data.buckets, xAxisData, yAxisData, value.data.missingData, this.elapsedMilliseconds());
    };
    return Histogram2DRenderer;
}(ui_1.Renderer));
exports.Histogram2DRenderer = Histogram2DRenderer;
//# sourceMappingURL=histogram2d.js.map