package org.hillview.test;

import org.hillview.dataset.api.Pair;

import java.util.ArrayList;
import java.util.List;

/*
    A class that optimizes the shape of a trellis display.
    @param x_max: The width of the display in pixels.
    @param x_min: Minimum width of a single histogram in pixels.
    @param y_max: The height of the display in pixels.
    @param y_min: Minimum height of a single histogram in pixels.
    @param max_ratio: The maximum aspect ratio we want in our histograms.
    x_min and y_min should satisfy the aspect ratio condition:
    Max(x_min/y_min, y_min/x_min) <= max_ratio.
 */
public class ComputeTrellisShape {
    public final int x_max;
    public final int x_min;
    public final int y_max;
    public final int y_min;
    public double max_ratio;
    public final int max_width;
    public final int max_height;


    public ComputeTrellisShape(int x_max, int x_min, int y_max, int y_min, double max_ratio) {
        this.x_max = x_max;
        this.x_min = x_min;
        this.y_max = y_max;
        this.y_min = y_min;
        this.max_ratio = max_ratio;
        this.max_width = (int) Math.floor(x_max/x_min);
        this.max_height = (int) Math.floor(y_max/y_min);
        if (Math.max(((double) x_min)/y_min, ((double) y_min)/x_min) > max_ratio)
            System.out.printf("The minimum sizes do not satisfy aspect ratio\n");
    }
    /*
    A class that describes the shape of trellis display.
    @param x_num: The number of histograms in a row.
    @param x_len: The width of a histogram in pixels.
    @param y_num: The number of histograms in a column.
    @param y_len: The height of a histogram in pixels.
    @param coverage: The fraction of available display used by the trellis display. This is the
    parameter that our algorithm optimizes, subject to constraints on the aspect ratio and minimum
    width and height of each histogram. This should be a fraction between 0 and 1. A value larger
    than 1 indicates that there is no feasible solution.
    */
    public static class TrellisShape {
        public int x_num;
        public double x_len;
        public int y_num;
        public double y_len;
        public double coverage;

        public TrellisShape(int x_num, double x_len, int y_num, double y_len,
                            double coverage) {
            this.x_num = x_num;
            this.x_len = x_len;
            this.y_num = y_num;
            this.y_len = y_len;
            this.coverage = coverage;
        }
    }

    public TrellisShape getShape(int n) {
        double total = this.x_max * this.y_max;
        double used = (double) n*this.x_min*this.y_min;
        double coverage = used/total;
        TrellisShape opt = new TrellisShape(this.max_width, this.x_min, this.max_height, this.y_min,
                coverage);
        if (this.max_width*this.max_height < n) {
            return opt;
        }
        List<Pair<Integer, Integer>> sizes = new ArrayList<>();
        for (int i = 1; i <= this.max_width; i++)
            for (int j = 1; j <= this.max_height; j++)
                if (i*j >= n)
                    sizes.add(new Pair(i, j));
        double x_len, y_len;
        for (Pair<Integer, Integer> size: sizes) {
            x_len = Math.floor(((double) this.x_max)/size.first);
            y_len = Math.floor(((double) this.y_max)/size.second);
            if (x_len >= y_len)
                x_len = Math.min(x_len, this.max_ratio*y_len);
            else
                y_len = Math.min(y_len, this.max_ratio*x_len);
            used = n * x_len * y_len;
            coverage = used/total;
            if ((x_len >= this.x_min) && (y_len >= this.y_min) && (coverage > opt.coverage)) {
                opt.x_len = x_len;
                opt.x_num = size.first;
                opt.y_len = y_len;
                opt.y_num = size.second;
                opt.coverage = coverage;
            }
        }
        return opt;
    }
}
