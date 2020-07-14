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

package org.hillview.table.api;

import org.hillview.dataset.api.IJson;

import java.io.Serializable;

/**
 * Represents an interval with two numeric endpoints.
 */
public class Interval implements Serializable, IJson {
    static final long serialVersionUID = 1;

    final double start;
    final double end;

    public Interval(double start, double end) {
        this.start = start;
        this.end = end;
    }

    public static final Interval minimumValue =
            new Interval(-java.lang.Double.MAX_VALUE, -java.lang.Double.MAX_VALUE);

    public static String toString(Double start, Double end) {
        return "[" + start + " : " + end + "]";
    }

    @Override
    public String toString() {
        return Interval.toString(this.start, this.end);
    }

    public double get(boolean start) {
        if (start)
            return this.start;
        else
            return this.end;
    }

    public int compareTo(Interval other) {
        int c = Double.compare(this.start, other.start);
        if (c != 0)
            return c;
        return Double.compare(this.end, other.end);
    }
}
