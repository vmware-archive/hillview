/* automatically generated from ../web/src/main/webapp/ui/helpUrl.src by doc/number-sections.py*/
/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import {ViewKind} from "./ui";

export const helpUrl: Map<ViewKind, string> = new Map<ViewKind, string>()
    .set("Table", "(#33-table-views)")
    .set("Histogram", "(#41-uni-dimensional-histogram-views)")
    .set("2DHistogram", "(#43-two-dimensional-histogram-views)")
    .set("Heatmap", "(#44-heatmap-views)")
    .set("QuartileVector", "(#42-quartiles-view-for-histogram-buckets)")
    .set("TrellisHistogram", "(#51-trellis-plots-of-1d-histograms)")
    .set("TrellisHeatmap", "(#53-trellis-plots-of-heatmaps)")
    .set("Trellis2DHistogram", "(#52-trellis-plots-of-2d-histograms)")
    .set("TrellisQuartiles", "(#54-trellis-plots-of-quartile-vectors)")
    .set("HeavyHitters", "(#34-frequent-elements-views)")
    .set("Schema", "(#32-schema-views)")
    .set("Load", "(#23-loading-data)")
    .set("SVD Spectrum", "(#45-singular-value-spectrum-views)")
    .set("CorrelationHeatmaps", "(#correlation-heatmaps)");
