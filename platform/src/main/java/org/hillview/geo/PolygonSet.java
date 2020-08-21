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

package org.hillview.geo;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PolygonSet {
    protected final SimpleFeatureSource source;
    protected final Set<String> columnNames;
    protected final Filter filter;
    protected final FilterFactory2 ffactory = CommonFactoryFinder.getFilterFactory2();
    protected final String geometryFeature;
    @Nullable
    protected final CoordinateReferenceSystem crs;

    public PolygonSet(String shapeFileName, String... columnNames) throws IOException {
        this.columnNames = new HashSet<String>(Arrays.asList(columnNames));
        this.filter = Filter.INCLUDE;
        File file = new File(shapeFileName);
        FileDataStore store = FileDataStoreFinder.getDataStore(file);
        FeatureType schema = store.getSchema();
        this.geometryFeature = schema.getGeometryDescriptor().getLocalName();
        String typeName = store.getTypeNames()[0];
        this.source = store.getFeatureSource(typeName);
        this.crs = schema.getCoordinateReferenceSystem();
    }

    private PolygonSet(PolygonSet source, Filter filter) {
        this.columnNames = source.columnNames;
        this.filter = ffactory.and(source.filter, filter);
        this.source = source.source;
        this.geometryFeature = source.geometryFeature;
        this.crs = source.crs;
    }

    public Set<String> columnNames() {
        return this.columnNames;
    }

    protected SimpleFeatureCollection getCollection(Filter filter) {
        try {
            filter = ffactory.and(this.filter, filter);
            return this.source.getFeatures(filter);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected SimpleFeatureCollection getCollection() {
        try {
            return this.source.getFeatures(this.filter);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String toJSON() {
        try {
            FeatureJSON json = new FeatureJSON();
            StringWriter writer = new StringWriter();
            json.writeFeatureCollection(this.getCollection(), writer);
            return writer.toString();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Bounding box of the geometric feature in the specified column.
     */
    public ReferencedEnvelope boundingBox() {
        return this.getCollection(filter).getBounds();
    }

    public PolygonSet find(String columnName, String value) {
        if (!this.columnNames.contains(columnName))
            throw new RuntimeException("Key " + columnName + " not legal");
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        Filter filter = ff.equals(ff.property(columnName), ff.literal(value));
        return new PolygonSet(this, filter);
    }

    public PolygonSet shrink(ReferencedEnvelope to) {
        Filter filter = ffactory.bbox(ffactory.property(this.geometryFeature), to);
        return new PolygonSet(this, filter);
    }

    public int size() {
        return this.getCollection().size();
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        String mainCol = this.columnNames.iterator().next();
        try (SimpleFeatureIterator it = this.getCollection().features()) {
            while (it.hasNext()) {
                SimpleFeature feature = it.next();
                result.append(feature.getAttribute(mainCol)).append(System.lineSeparator());
            }
        }
        return result.toString();
    }
}
