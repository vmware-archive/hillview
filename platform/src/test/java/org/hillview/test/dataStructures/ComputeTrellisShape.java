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

package org.hillview.test.dataStructures;

import org.hillview.dataset.api.Pair;

import java.util.ArrayList;
import java.util.List;

class ComputeTrellisShape {
    private final int x_max;
    private final int x_min;
    private final int y_max;
    private final int y_min;
    private final double max_ratio;
    private final int header_ht;
    private final int max_width;
    private final int max_height;

    /**
     * A class that optimizes the shape of a trellis display.
     * @param x_max: The width of the display in pixels.
     * @param x_min: Minimum width of a single histogram in pixels.
     * @param y_max: The height of the display in pixels.
     * @param y_min: Minimum height of a single histogram in pixels.
     * @param max_ratio: The maximum aspect ratio we want in our histograms.
     * @param header_ht: The header height for each window.
     * x_min and y_min should satisfy the aspect ratio condition:
     * Max(x_min/y_min, y_min/x_min) <= max_ratio.
     */
    ComputeTrellisShape(int x_max, int x_min, int y_max, int y_min, double max_ratio,
                               int header_ht) {
        this.x_max = x_max;
        this.x_min = x_min;
        this.y_max = y_max;
        this.y_min = y_min;
        this.max_ratio = max_ratio;
        this.header_ht = header_ht;
        this.max_width = x_max / x_min;
        this.max_height = y_max / (y_min + header_ht);
        if (Math.max(((double) x_min)/y_min, ((double) y_min)/x_min) > max_ratio)
            System.out.println("The minimum sizes do not satisfy aspect ratio");
    }

    static class TrellisShape {
        int x_num;
        double x_len;
        int y_num;
        double y_len;
        double coverage;

        /**
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
        TrellisShape(int x_num, double x_len, int y_num, double y_len, double coverage) {
            this.x_num = x_num;
            this.x_len = x_len;
            this.y_num = y_num;
            this.y_len = y_len;
            this.coverage = coverage;
        }
    }

    TrellisShape getShape(int nBuckets) {
        double total = this.x_max * this.y_max;
        double used = (double)nBuckets * this.x_min * (this.y_min + this.header_ht);
        double coverage = used/total;
        TrellisShape opt = new TrellisShape(
                this.max_width, this.x_min, this.max_height, this.y_min,
                coverage);
        if (this.max_width*this.max_height < nBuckets) {
            return opt;
        }
        List<Pair<Integer, Integer>> sizes = new ArrayList<Pair<Integer, Integer>>();
        for (int i = 1; i <= this.max_width; i++)
            for (int j = 1; j <= this.max_height; j++)
                if (i*j >= nBuckets)
                    sizes.add(new Pair<Integer, Integer>(i, j));
        double x_len, y_len;
        for (Pair<Integer, Integer> size: sizes) {
            assert size.first != null;
            assert size.second != null;
            x_len = Math.floor(((double) this.x_max)/size.first);
            y_len = Math.floor(((double) this.y_max)/size.second) - this.header_ht;
            if (x_len >= y_len)
                x_len = Math.min(x_len, this.max_ratio * y_len);
            else
                y_len = Math.min(y_len, this.max_ratio*x_len);
            used = nBuckets * x_len * (y_len + this.header_ht);
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
