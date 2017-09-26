package org.hillview.sketches;

import org.hillview.dataset.api.IJson;

public class Point2D implements IJson {
    public Point2D(double x, double y) {
        this.x = x;
        this.y = y;
    }
    public double x;
    public double y;

    @Override
    public String toString() {
        return String.format("(%.2f, %.2f)", x, y);
    }
}