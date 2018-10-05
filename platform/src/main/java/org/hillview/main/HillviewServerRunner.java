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

package org.hillview.main;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.utils.HostList;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.HostAndPort;

/**
 * Starts a Hillview server.  Depending on the command-line arguments it
 * will start either a worker node, or an aggregator node that talks to many worker nodes.
 */
class HillviewServerRunner {
    private static void usage() {
        System.out.println("Invalid number of arguments.\n" +
                "Usage: java -jar <jarname> [children] host:port\n" +
                "`host:port` is the address where the service receives requests.\n" +
                "`children` is an optional file that contains a list of children nodes.\n" +
                "           Each child is of the form host:port.\n" +
                "           If present this server will work as an aggregator node.\n"
        );
    }

    public static void main(String[] args) {
        try {
            String hostAndPort;
            IDataSet<Empty> initial;

            if (args.length == 1) {
                // worker node
                HillviewLogger.initialize("worker", "hillview.log");
                initial = new LocalDataSet<Empty>(Empty.getInstance());
                hostAndPort = args[0];
            } else if (args.length == 2) {
                // aggregator node
                HillviewLogger.initialize("aggregator", "hillview-agg.log");
                HostList cluster = HostList.fromFile(args[0]);
                initial = RemoteDataSet.createCluster(cluster, RemoteDataSet.defaultDatasetIndex);
                hostAndPort = args[1];
            } else {
                usage();
                return;
            }

            new HillviewServer(HostAndPort.fromString(hostAndPort), initial);
            HillviewLogger.instance.info("Created HillviewServer");
            Thread.currentThread().join();
        } catch (Exception ex) {
            HillviewLogger.instance.error("Caught exception; exiting", ex);
        }
    }
}
