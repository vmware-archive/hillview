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
var ui_1 = require("./ui");
var d3 = require('d3');
var rpc_1 = require("./rpc");
var util_1 = require("./util");
var HistogramViewBase = (function (_super) {
    __extends(HistogramViewBase, _super);
    function HistogramViewBase(remoteObjectId, tableSchema, page) {
        var _this = this;
        _super.call(this, remoteObjectId);
        this.tableSchema = tableSchema;
        // When plotting integer values we increase the data range by .5 on the left and right.
        // The adjustment is the number of pixels on screen that we "waste".
        // I.e., the cdf plot will start adjustment/2 pixels from the chart left margin
        // and will end adjustment/2 pixels from the right margin.
        this.adjustment = 0;
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = function (e) { return _this.keyDown(e); };
        this.dragging = false;
        this.setPage(page);
        this.topLevel.tabIndex = 1;
        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);
        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
        var position = document.createElement("table");
        this.topLevel.appendChild(position);
        position.className = "noBorder";
        var body = position.createTBody();
        var row = body.insertRow();
        row.className = "noBorder";
        var infoWidth = "150px";
        var labelCell = row.insertCell(0);
        labelCell.width = infoWidth;
        this.xLabel = document.createElement("div");
        this.xLabel.style.textAlign = "left";
        labelCell.appendChild(this.xLabel);
        labelCell.className = "noBorder";
        labelCell = row.insertCell(1);
        labelCell.width = infoWidth;
        this.yLabel = document.createElement("div");
        this.yLabel.style.textAlign = "left";
        labelCell.appendChild(this.yLabel);
        labelCell.className = "noBorder";
        labelCell = row.insertCell(2);
        labelCell.width = infoWidth;
        this.cdfLabel = document.createElement("div");
        this.cdfLabel.style.textAlign = "left";
        labelCell.appendChild(this.cdfLabel);
        labelCell.className = "noBorder";
    }
    HistogramViewBase.prototype.keyDown = function (ev) {
        if (ev.keyCode == ui_1.KeyCodes.escape)
            this.cancelDrag();
    };
    HistogramViewBase.prototype.cancelDrag = function () {
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    };
    HistogramViewBase.prototype.getHTMLRepresentation = function () {
        return this.topLevel;
    };
    HistogramViewBase.prototype.dragStart = function () {
        this.dragging = true;
        var position = d3.mouse(this.chart.node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    };
    HistogramViewBase.prototype.dragMove = function () {
        this.onMouseMove();
        if (!this.dragging)
            return;
        var ox = this.selectionOrigin.x;
        var position = d3.mouse(this.chart.node());
        var x = position[0];
        var width = x - ox;
        var height = this.chartResolution.height;
        if (width < 0) {
            ox = x;
            width = -width;
        }
        this.selectionRectangle
            .attr("x", ox + HistogramViewBase.margin.left)
            .attr("y", HistogramViewBase.margin.top)
            .attr("width", width)
            .attr("height", height);
    };
    HistogramViewBase.prototype.dragEnd = function () {
        if (!this.dragging)
            return;
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        var position = d3.mouse(this.chart.node());
        var x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
    };
    HistogramViewBase.prototype.setPage = function (page) {
        if (page == null)
            throw ("null FullPage");
        this.page = page;
    };
    HistogramViewBase.prototype.getPage = function () {
        if (this.page == null)
            throw ("Page not set");
        return this.page;
    };
    HistogramViewBase.getRenderingSize = function (page) {
        var width = page.getWidthInPixels();
        width = width - HistogramViewBase.margin.left - HistogramViewBase.margin.right;
        var height = HistogramViewBase.chartHeight - HistogramViewBase.margin.top - HistogramViewBase.margin.bottom;
        return { width: width, height: height };
    };
    HistogramViewBase.bucketCount = function (stats, page, columnKind) {
        var size = HistogramViewBase.getRenderingSize(page);
        var bucketCount = HistogramViewBase.maxBucketCount;
        if (size.width / HistogramViewBase.minBarWidth < bucketCount)
            bucketCount = size.width / HistogramViewBase.minBarWidth;
        if (columnKind == "Integer" ||
            columnKind == "Category") {
            bucketCount = Math.min(bucketCount, stats.max - stats.min + 1);
        }
        return bucketCount;
    };
    HistogramViewBase.categoriesInRange = function (stats, bucketCount, allStrings) {
        var boundaries = null;
        var max = Math.floor(stats.max);
        var min = Math.ceil(stats.min);
        var range = max - min;
        if (range <= 0)
            bucketCount = 1;
        if (allStrings != null) {
            if (bucketCount >= range) {
                boundaries = allStrings.slice(min, max + 1); // slice end is exclusive
            }
            else {
                boundaries = [];
                for (var i = 0; i <= bucketCount; i++) {
                    var index = min + Math.round(i * range / bucketCount);
                    boundaries.push(allStrings[index]);
                }
            }
        }
        return boundaries;
    };
    HistogramViewBase.invertToNumber = function (v, scale, kind) {
        var inv = scale.invert(v);
        var result = 0;
        if (kind == "Integer" || kind == "Category") {
            result = Math.round(inv);
        }
        else if (kind == "Double") {
            result = inv;
        }
        else if (kind == "Date") {
            result = util_1.Converters.doubleFromDate(inv);
        }
        return result;
    };
    HistogramViewBase.maxBucketCount = 40;
    HistogramViewBase.minBarWidth = 5;
    HistogramViewBase.minChartWidth = 200; // pixels
    HistogramViewBase.chartHeight = 400; // pixels
    HistogramViewBase.margin = {
        top: 50,
        right: 30,
        bottom: 40,
        left: 40
    };
    return HistogramViewBase;
}(rpc_1.RemoteObject));
exports.HistogramViewBase = HistogramViewBase;
//# sourceMappingURL=histogramBase.js.map