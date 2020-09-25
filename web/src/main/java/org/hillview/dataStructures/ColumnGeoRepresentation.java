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

package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.geo.PolygonSet;
import org.hillview.utils.JsonInString;

import java.io.Serializable;

/**
 * Describes how a column in a table is mapped
 * to a geographic dataset.
 */
@SuppressWarnings("NotNullFieldNotInitialized")
public class ColumnGeoRepresentation implements Serializable, IJson {
    public String columnName; // e.g., OriginState
    public String property; // which property in the dataset is indexed by values in the column. e.g., STUSPS
    public String projection; // one of the supported data projections
    // Legal projection names are:
    // geoAzimuthalEqualArea
    // geoAzimuthalEquidistant
    // geoGnomonic
    // geoOrthographic
    // geoStereographic
    // geoEqualEarth
    // geoAlbersUsa
    // geoConicEqualArea
    // geoConicEquidistant
    // geoEquirectangular
    // geoMercator
    // geoTransverseMercator
    // geoNaturalEarth1

    public JsonInString createJSON(PolygonSet ps) {
        return new JsonInString(
                "{" +
                        "columnName:" + this.columnName + ",\n" +
                        "property:" + this.property + ",\n" +
                        "projection:" + this.projection + ",\n" +
                        "data:" + ps.toJSON()
                        + "}"
        );
    }
}
