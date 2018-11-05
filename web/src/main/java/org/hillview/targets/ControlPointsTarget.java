/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.RpcTarget;
import org.hillview.sketches.Centroids;
import org.hillview.table.SmallTable;
import org.hillview.utils.Converters;
import org.hillview.utils.Point2D;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.MetricMDS;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

// All RpcTarget objects must be public
@SuppressWarnings("WeakerAccess")
public final class ControlPointsTarget extends RpcTarget {
    final DoubleMatrix highDimData;
    @Nullable
    private DoubleMatrix lowDimData;

    ControlPointsTarget(SmallTable table, String[] colNames, HillviewComputation computation) {
        super(computation);
        this.highDimData = BlasConversions.toDoubleMatrix(table, Arrays.asList(colNames));
        this.registerObject();
    }

    ControlPointsTarget(Centroids<String> centroids, HillviewComputation computation) {
        super(computation);
        HashMap<String, double[]> map = centroids.computeCentroids();
        Set<String> keys = map.keySet();
        int numCols = map.get(new ArrayList<String>(keys).get(0)).length;
        int numCentroids = map.size();
        this.highDimData = new DoubleMatrix(numCentroids, numCols);
        int i = 0;
        for (String key : keys) {
            double[] centroid = map.get(key);
            for (int j = 0; j < centroid.length; j++)
                this.highDimData.put(i, j, centroid[j]);
            i++;
        }
        this.registerObject();
    }

    TableTarget.ControlPoints2D mds(int seed) {
        MetricMDS mds = new MetricMDS(this.highDimData);
        this.lowDimData = mds.computeEmbedding(seed);

        Point2D[] points = new Point2D[this.lowDimData.rows];
        for (int i = 0; i < this.lowDimData.rows; i++) {
            points[i] = new Point2D(this.lowDimData.get(i, 0), this.lowDimData.get(i, 1));
        }
        return new TableTarget.ControlPoints2D(points);
    }

    public void setControlPoints(Point2D[] points) {
        Converters.checkNull(this.lowDimData);
        for (int i = 0; i < points.length; i++) {
            this.lowDimData.put(i, 0, points[i].x);
            this.lowDimData.put(i, 1, points[i].y);
        }
    }
}
