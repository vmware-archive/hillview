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
 */

package org.hillview.dataset.api;

import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

/**
 * Base class for all IDataSets.
 * @param <T>  Type of data in dataset.
 */
public abstract class BaseDataSet<T> implements IDataSet<T> {
    static int uniqueId = 0;
    protected final int id;

    public BaseDataSet() {
        this.id = BaseDataSet.uniqueId++;
    }

    @Override
    public String toString() {
        String host = Utilities.getHostName();
        return this.getClass().getName() + "(" + this.id + ")@" + host;
    }

    /**
     * Helper function which can be invoked in a map over streams to log the processing
     * over each stream element.
     */
    protected <S> S logPipe(S data, String message) {
        HillviewLogger.instance.info("logPipe", "{0}:{1}", this.toString(), message);
        return data;
    }
}
