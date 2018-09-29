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
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * This class represents a subset of the cluster configuration information.
 */
public class ClusterConfig {
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
     * True if we need to delete log files when deploying.
     */
    public boolean cleanup;

    private void validate() {
        if (this.webserver == null)
            throw new RuntimeException("webserver not defined");
        if (this.workers == null)
            throw new RuntimeException("workers not defined");
        if (this.worker_port == -1)
            throw new RuntimeException("worker_port not defined");
        if (this.user == null)
            throw new RuntimeException("user not defined");
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
}
