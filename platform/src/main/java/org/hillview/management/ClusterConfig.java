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

package org.hillview.management;

import org.hillview.dataset.api.IJson;
import org.hillview.utils.HostAndPort;
import org.hillview.utils.HostList;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a subset of the cluster configuration information.
 */
@SuppressWarnings("CanBeFinal")
public class ClusterConfig {
    public static class AggregatorConfig {
        @Nullable
        String name;
        String[] workers;
    }

    /**
     * Host running the web server.
     */
    @Nullable
    public String webserver;
    /**
     * Hosts running the back-end workers.
     */
    @Nullable
    public String[] workers;
    /**
     * Aggregators in front of the workers
     */
    @Nullable
    public AggregatorConfig[] aggregators;
    /**
     * Folder where the hillview service is installed on each worker.
     */
    @Nullable
    public String service_folder;
    /**
     * User account that runs the hillview workers and web server.
     */
    @Nullable
    public String user;
    /**
     * Network port used by backend workers.
     */
    public int worker_port = -1;
    /**
     * Network port used by aggregators.
     */
    public int aggregator_port = -1;
    /**
     * True if we need to delete log files when deploying.
     */
    public boolean cleanup;

    private void validate() {
        if (this.webserver == null)
            throw new RuntimeException("webserver not defined");
        if (this.getWorkers() == null)
            throw new RuntimeException("workers not defined");
        if (this.worker_port == -1)
            throw new RuntimeException("worker_port not defined");
        if (this.user == null)
            this.user = System.getProperty("user.name");
        if (this.user == null)
            throw new RuntimeException("Cannot find current user name");
        // Other fields are not mandatory for now
    }

    private static String removeComment(String s) {
        int index = s.indexOf("//");
        if (index < 0)
            return s;
        return s.substring(0, index);
    }

    /**
     * Parse a cluster configuration file and create a Java
     * ClusterConfig object.
     */
    public static ClusterConfig parse(String file) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(file));
        lines = Linq.map(lines, ClusterConfig::removeComment);
        String contents = String.join(" ", lines);
        ClusterConfig result = IJson.gsonInstance.fromJson(contents, ClusterConfig.class);
        result.validate();
        return result;
    }

    public HostList getWorkers() {
        List<HostAndPort> workers = new ArrayList<HostAndPort>();
        if (this.workers != null) {
            for (String w : this.workers)
                 workers.add(new HostAndPort(w, this.worker_port));
        } else {
            assert this.aggregators != null;
            for (AggregatorConfig a : this.aggregators) {
                for (String w : a.workers)
                    workers.add(new HostAndPort(w, this.worker_port));
            }
        }
        return new HostList(workers);
    }

    public HostList getAggregators() {
        List<HostAndPort> agg = new ArrayList<HostAndPort>();
        if (this.aggregators == null)
            return new HostList(agg);
        for (AggregatorConfig a : this.aggregators) {
            assert a.name != null;
            agg.add(new HostAndPort(a.name, this.aggregator_port));
        }
        return new HostList(agg);
    }
}
