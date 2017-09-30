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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This is a base class for management messages which are interpreted by various
 * DataSet implementations in special ways.  The management messages traverse
 * the IDataSet tree and are executed in post-order, starting from the leaves,
 * on the return path through the IDataSet.  Each layer appends new status messages
 * if it does anything interesting.
 */
public class ControlMessage implements Serializable {
    public static class Status implements Serializable, IJson {
        /**
         * Host where control message executed.
         */
        String hostname;
        /**
         * Some report about the execution status.
         */
        String result;
        /**
         * Exception caused if any.
         */
        @Nullable
        Throwable exception;

        public Status(String result) {
            this.hostname = Utilities.getHostName();
            this.result = result;
            this.exception = null;
        }

        public Status(String result, Throwable ex) {
            this.hostname = Utilities.getHostName();
            this.result = result;
            this.exception = ex;
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject result = new JsonObject();
            result.addProperty("hostname", this.hostname);
            result.addProperty("result", this.result);
            result.addProperty("exception", Utilities.throwableToString(this.exception));
            return result;
        }
    }

    /**
     * This is executed at all LocalDataSet objects.
     * @return null if there is nothing to do.
     */
    @Nullable
    public <T> Status localAction(LocalDataSet<T> dataset) { return null; }

    /**
     * This is executed at all ParallelDataSet objects
     * @return null if there is nothing to do.
     */
    @Nullable
    public <T> Status parallelAction(ParallelDataSet<T> dataset) { return null; }

    /**
     * This is executed at all RemoteDataSet objects (client-side).
     * @return null if there is nothing to do.
     */
    @Nullable
    public <T> Status remoteAction(RemoteDataSet<T> dataset) { return null; }

    /**
     * This is executed at all HillviewServer objects (server-side for RemoteDataSets).
     * @return null if there is nothing to do.
     */
    @Nullable
    public Status remoteServerAction(HillviewServer server) { return null; }
}
