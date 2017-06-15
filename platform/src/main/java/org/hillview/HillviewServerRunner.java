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

package org.hillview;

import com.google.common.net.HostAndPort;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.remoting.ClusterDescription;
import org.hillview.remoting.HillviewServer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Brings up a single instance of a HillviewServer
 */
public class HillviewServerRunner {
    @Nullable private static IDataSet<Empty> dataSet = null;

    private static void initialize(final ClusterDescription description) {
        final int numServers = description.getServerList().size();
        assert numServers > 0;
        if (numServers > 1) {
            System.out.println("Creating a PDS");
            final ArrayList<IDataSet<Empty>> emptyDatasets = new ArrayList<>(numServers);
            description.getServerList().forEach(server -> emptyDatasets.add(new RemoteDataSet<>(server)));
            dataSet = new ParallelDataSet<>(emptyDatasets);
        }
        else {
            dataSet = new LocalDataSet<>(Empty.getInstance());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final IDataSet<Empty> dataSet = new LocalDataSet<>(Empty.getInstance());
        if (args.length != 1) {
            System.out.println("Invalid number of arguments.\n" +
                               "Usage: java -jar <jarname> <HillviewServer listen address>");
            System.exit(1);
        }
        final String hostnameAndPort = args[0];
        System.out.println("Starting HillviewServer to listen on " + hostnameAndPort);
        final HillviewServer server = new HillviewServer(HostAndPort.fromString(hostnameAndPort), dataSet);
        Thread.currentThread().join();
    }
}