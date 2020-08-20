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

package org.hillview.test.storage;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.hillview.geo.PolygonSet;
import org.hillview.test.BaseTest;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class GeoAPITest extends BaseTest {
    String geoDir = dataDir + "/geo/";

    private SimpleFeatureCollection getData() throws IOException {
        File file = new File(geoDir + "us_states/cb_2019_us_state_20m.shp");
        FileDataStore store = FileDataStoreFinder.getDataStore(file);
        String typeName = store.getTypeNames()[0];
        SimpleFeatureSource source = store.getFeatureSource(typeName);
        return source.getFeatures();
    }

    @Test
    public void testStates() throws IOException {
        SimpleFeatureCollection collection = this.getData();
        Assert.assertEquals(52, collection.size());
        HashSet<String> states = new HashSet<String>(Utilities.list(
                "AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID"
                ,"IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC"
                ,"ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","PR","RI","SC"
                ,"SD","TN","TX","UT","VA","VT","WA","WI","WV","WY"));
        Assert.assertEquals(52, states.size());

        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                String state = (String)feature.getAttribute("STUSPS");
                Assert.assertTrue(states.contains(state));
            }
        }
    }

    @Test
    public void dumpPropertiesTest() throws IOException {
        SimpleFeatureCollection collection = this.getData();
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            if (features.hasNext()) {
                SimpleFeature feature = features.next();
                for (Property p: feature.getProperties()) {
                    if (toPrint)
                        System.out.println(p.getName() + ":" + p.getType() + ":" + p.getValue());
                }
            }
        }
    }

    private PolygonSet getStates() throws IOException {
        PolygonSet shp = new PolygonSet(
                geoDir + "us_states/cb_2019_us_state_20m.shp", "STUSPS", "NAME");
        Assert.assertNotNull(shp);
        return shp;
    }

    @Test
    public void boundingBoxTest() throws IOException {
        PolygonSet shp = this.getStates();
        BoundingBox boundingBox = shp.boundingBox();
        Assert.assertNotNull(boundingBox);
        if (toPrint)
            System.out.println(boundingBox);
    }

    @Test
    public void testRetrieval() throws IOException {
        PolygonSet shp = this.getStates();
        PolygonSet set = shp.find("STUSPS", "WA");
        Assert.assertEquals(1, set.size());

        set = shp.find("STUSPS", "CA");
        Assert.assertEquals(1, set.size());

        PolygonSet set1 = shp.find("NAME", "California");
        Assert.assertEquals(1, set.size());
        if (toPrint)
            System.out.println(set.toJSON());
        Assert.assertEquals(set.toJSON(), set1.toJSON());

        set = shp.find("STUSPS", "Not a state");
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testRectangle() throws IOException {
        PolygonSet shapes = this.getStates();
        Assert.assertTrue(shapes.size() > 0);
        ReferencedEnvelope box = shapes.boundingBox();
        double area = box.getArea();
        ReferencedEnvelope box1 = new ReferencedEnvelope(box);
        box1.expandBy(-box.getWidth() / 4, -box.getHeight() / 4);
        double area0 = box1.getArea();
        Assert.assertEquals(4 * area0, area, .1);

        PolygonSet shapes1 = shapes.shrink(box1);
        Assert.assertTrue(shapes1.size() > 0);
        Assert.assertTrue(shapes1.size() < shapes.size());

        PolygonSet ak = shapes1.find("STUSPS", "AK");
        Assert.assertEquals(0, ak.size());
        PolygonSet ma = shapes1.find("NAME", "Maine");
        Assert.assertNotNull(ma);
        String json = ma.toJSON();
        Assert.assertNotNull(json);
        if (toPrint)
            System.out.println(json);
    }
}
