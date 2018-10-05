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

package org.hillview.test;

import org.hillview.management.ClusterConfig;
import org.hillview.utils.HostList;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test the parser for the cluster config grammar.
 */
public class ConfigGrammarTest extends BaseTest {
    @Test
    public void testConfigLoading() throws IOException {
        ClusterConfig config = ClusterConfig.parse("../bin/config.json");
        Assert.assertEquals("web.server.name", config.webserver);
        HostList workers = config.getWorkers();
        Assert.assertNotNull(workers);
        Assert.assertEquals(4, workers.size());
        Assert.assertNotNull(config.aggregators);
        Assert.assertEquals(2, config.aggregators.length);
        Assert.assertEquals("worker1.name", workers.get(0).host);
        Assert.assertEquals(3569, config.worker_port);
        Assert.assertEquals("/home/hillview", config.service_folder);
    }
}
