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

import {CategoricalValues, IDistinctStrings} from "./javaBridge";

/**
 * All strings that can appear in a categorical column.
 */
export class DistinctStrings implements IDistinctStrings {
    public uniqueStrings: string[] | null;
    // This may be true if there are too many distinct strings in a column.
    public truncated: boolean;

    public constructor(ds: IDistinctStrings | null, protected colName: string) {
        if (ds == null) {
            this.uniqueStrings = null;
        } else {
            this.truncated = ds.truncated;
            this.uniqueStrings = ds.uniqueStrings;
            this.uniqueStrings.sort();
        }
    }

    public size(): number { return this.uniqueStrings.length; }

    public getCategoricalValues(): CategoricalValues {
        return new CategoricalValues(this.colName, this.uniqueStrings);
    }

    /**
     * Returns all strings numbered between min and max.
     * @param min    Minimum string number
     * @param max    Maximum string number
     * @param bucketCount  Number of buckets.
     * @returns      null if there are no strings (e.g., this is not
     *               a categorical column)
     */
    public categoriesInRange(min: number, max: number, bucketCount: number): string[] | null {
        if (this.uniqueStrings == null)
            return null;

        let boundaries: string[] = null;
        if (min <= 0)
            min = 0;
        if (max >= this.uniqueStrings.length - 1)
            max = this.uniqueStrings.length - 1;
        max = Math.floor(max);
        min = Math.ceil(min);
        let range = max - min;
        if (range <= 0)
            bucketCount = 1;

        if (this.uniqueStrings != null) {
            if (bucketCount >= range) {
                boundaries = this.uniqueStrings.slice(min, max + 1);  // slice end is exclusive
            } else {
                boundaries = [];
                for (let i = 0; i <= bucketCount; i++) {
                    let index = min + Math.round(i * range / bucketCount);
                    boundaries.push(this.uniqueStrings[index]);
                }
            }
        }
        return boundaries;
    }

    public get(index: number): string {
        index = Math.round(<number>index);
        if (index >= 0 && index < this.uniqueStrings.length)
            return this.uniqueStrings[index];
        return null;
    }
}
