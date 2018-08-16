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

export class DistinctStrings {
    public constructor(public readonly uniqueStrings: string[],
                       protected colName: string) {
        // we expect these strings to be always in sorted order
        this.uniqueStrings = uniqueStrings;
    }

    public size(): number { return this.uniqueStrings.length; }

    /**
     * Returns count strings periodically spaced
     * @param count  Number of buckets.
     * @returns      null if there are no strings (e.g., this is not
     *               a categorical column)
     */
    public periodicSamples(count: number): string[] | null {
        if (this.uniqueStrings == null)
            return null;

        let boundaries: string[] = null;
        if (this.uniqueStrings != null) {
            if (count >= this.uniqueStrings.length) {
                return this.uniqueStrings;
            } else {
                boundaries = [];
                for (let i = 0; i <= count; i++) {
                    const index = Math.round(i * (this.uniqueStrings.length - 1) / count);
                    boundaries.push(this.uniqueStrings[index]);
                }
            }
        }
        return boundaries;
    }

    public get(index: number, clamp: boolean): string {
        index = Math.round(index);
        if (clamp) {
            if (index < 0)
                index = 0;
            if (index >= this.uniqueStrings.length)
                index = this.uniqueStrings.length - 1;
        }
        if (index >= 0 && index < this.uniqueStrings.length)
            return this.uniqueStrings[index];
        return null;
    }
}
