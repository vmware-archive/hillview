/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

export interface Pair<T1, T2> {
    first: T1;
    second: T2;
}

export interface Triple<T1, T2, T3> {
    first: T1;
    second: T2;
    third: T3;
}

// Direct counterpart of corresponding Java class
export class Converters {
    public static dateFromDouble(value: number): Date {
        return new Date(value);
    }

    public static doubleFromDate(value: Date): number {
        return value.getTime();
    }
}