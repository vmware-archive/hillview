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
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.remoting.HillviewServer;
import org.hillview.utils.HillviewLogManager;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Brings up a single instance of a HillviewServer
 */
public class HillviewServerRunner {

    static void usage() {
        System.out.println("Invalid number of arguments.\n" +
                "Usage: java -jar <jarname> <port listen address>");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HillviewLogManager.instance.initialize();
        try {
            final IDataSet<Empty> dataSet = new LocalDataSet<>(Empty.getInstance());
            if (args.length != 1) {
                usage();
                throw new RuntimeException("Incorrect arguments");
            }
            final String hostnameAndPort = args[0];
            final HillviewServer server = new HillviewServer(HostAndPort.fromString(hostnameAndPort), dataSet);
            HillviewLogManager.instance.logger.log(Level.INFO, "Created HillviewServer");
            Thread.currentThread().join();
        } catch (Exception ex) {
            HillviewLogManager.instance.logger.log(Level.SEVERE, "Unhandled exception", ex);
        }
    }
}