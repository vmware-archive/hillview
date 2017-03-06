/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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
 *
 */

package org.hiero.sketch;

import java.util.Comparator;
import java.util.Objects;

class MyCompare implements Comparator<Integer> {
    private MyCompare() {}

    static final MyCompare instance = new MyCompare();

    @Override
    public int compare(final Integer x, final Integer y) {
        if (x > y)
            return 1;
        else if (Objects.equals(x, y))
            return 0;
        else
            return -1;
    }
}
