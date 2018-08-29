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

import org.antlr.v4.runtime.*;
import org.hillview.ClusterConfigLexer;
import org.hillview.ClusterConfigParser;
import org.hillview.management.ClusterConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Test the parser for the cluster config grammar.
 */
public class ConfigGrammarTest extends BaseTest {
    @Test
    public void testAntlrGrammar() {
        String simplestProgram = "# comment\nwebserver = \"web.server.name\"";
        CharStream inputCharStream = CharStreams.fromString(simplestProgram);
        TokenSource tokenSource = new ClusterConfigLexer(inputCharStream);
        TokenStream inputTokenStream = new CommonTokenStream(tokenSource);
        ClusterConfigParser parser = new ClusterConfigParser(inputTokenStream);
        parser.addErrorListener(new ConsoleErrorListener());
        ClusterConfigParser.InitContext context = parser.init();
        List<ClusterConfigParser.AssignmentContext> assigns = context.assignment();
        Assert.assertEquals(1, assigns.size());
        ClusterConfigParser.AssignmentContext assign = assigns.get(0);
        Assert.assertEquals("webserver", assign.IDENTIFIER().getText());
        Assert.assertEquals("\"web.server.name\"", assign.expression().STRING().getText());
    }

    @Test
    public void testConfigLoading() throws IOException {
        ClusterConfig config = ClusterConfig.parse("../bin/config.py");
        Assert.assertEquals("web.server.name", config.webServer);
        Assert.assertNotNull(config.backends);
        Assert.assertEquals(2, config.backends.length);
        Assert.assertEquals("worker1.name", config.backends[0]);
        Assert.assertEquals(3569, config.backendPort);
        Assert.assertEquals("/home/hillview", config.serviceFolder);
    }
}
