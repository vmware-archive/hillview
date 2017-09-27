package org.hillview;

import org.hillview.sketches.Centroids;
import org.hillview.sketches.Point2D;
import org.hillview.sketches.RandomSampling;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.MetricMDS;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ControlPointsTarget extends RpcTarget {

    public final DoubleMatrix highDimData;
    @Nullable
    public DoubleMatrix lowDimData;

    public ControlPointsTarget(RandomSampling samples, List<String> colNames) {
        this.highDimData = BlasConversions.toDoubleMatrix(samples.table, colNames);
    }

    public ControlPointsTarget(Centroids<String> centroids) {
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
    }

    public TableTarget.ControlPoints2D mds(int seed) {
        MetricMDS mds = new MetricMDS(this.highDimData);
        this.lowDimData = mds.computeEmbedding(seed);

        Point2D[] points = new Point2D[this.lowDimData.rows];
        for (int i = 0; i < this.lowDimData.rows; i++) {
            points[i] = new Point2D(this.lowDimData.get(i, 0), this.lowDimData.get(i, 1));
        }
        return new TableTarget.ControlPoints2D(points);
    }

    public void setControlPoints(Point2D[] points) {
        for (int i = 0; i < points.length; i++) {
            this.lowDimData.put(i, 0, points[i].x);
            this.lowDimData.put(i, 1, points[i].y);
        }
    }
}

