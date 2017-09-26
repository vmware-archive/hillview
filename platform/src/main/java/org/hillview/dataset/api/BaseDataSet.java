/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import org.hillview.utils.HillviewLogging;

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
        String host = "?";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            host = localMachine.getHostName();
        } catch (java.net.UnknownHostException e) {
            HillviewLogging.logger.error("Cannot get host name");
        }
        return this.getClass().getName() + "(" + this.id + ")@" + host;
    }

    /**
     * Helper function which can be invoked in a map over streams to log the processing
     * over each stream element.
     */
    protected <S> S log(S data, String message) {
        this.log(message);
        return data;
    }

    protected void log(String message) {
        HillviewLogging.logger.info(this.toString() + ":" + message);
    }
}
