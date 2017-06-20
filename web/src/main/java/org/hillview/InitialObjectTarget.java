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

package org.hillview;

import com.google.common.net.HostAndPort;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LoadCsvFileMapper;
import org.hillview.remoting.ClusterDescription;
import org.hillview.remoting.HillviewServer;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class InitialObjectTarget extends RpcTarget {
    private static final String LOCALHOST = "127.0.0.1";
    private static final Logger logger = Logger.getLogger(InitialObjectTarget.class.getName());

    @Nullable
    private IDataSet<Empty> emptyDataset = null;

    InitialObjectTarget() {
        Empty empty = new Empty();
        final List<HostAndPort> hostAndPorts = new ArrayList<>();
        hostAndPorts.add(HostAndPort.fromParts(LOCALHOST, HillviewServer.DEFAULT_PORT));
        final ClusterDescription desc = new ClusterDescription(hostAndPorts);
        this.initialize(desc);
    }

    private void initialize(final ClusterDescription description) {
        final int numServers = description.getServerList().size();
        if (numServers <= 0) {
            throw new IllegalArgumentException("ClusterDescription must contain one or more servers");
        }
        if (numServers > 1) {
            logger.info("Creating PDS");
            final ArrayList<IDataSet<Empty>> emptyDatasets = new ArrayList<IDataSet<Empty>>(numServers);
            description.getServerList().forEach(server -> emptyDatasets.add(new RemoteDataSet<>(server)));
            this.emptyDataset = new ParallelDataSet<>(emptyDatasets);
        }
        else {
            logger.info("Creating RDS");
            this.emptyDataset = new RemoteDataSet<Empty>(description.getServerList().get(0));
        }
    }

    @HillviewRpc
    void initializeCluster(RpcRequest request, Session session) {
        ClusterDescription description = request.parseArgs(ClusterDescription.class);
        this.initialize(description);
    }

    @HillviewRpc
    void prepareFiles(RpcRequest request, Session session) {
        int which = request.parseArgs(Integer.class);

        Converters.checkNull(this.emptyDataset);
        this.runFlatMap(this.emptyDataset, new LoadCsvFileMapper(which), FileNamesTarget::new, request, session);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }
}
