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

import org.antlr.v4.runtime.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.hillview.ClusterConfigLexer;
import org.hillview.ClusterConfigParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a subset of the cluster configuration information.
 */
public class ClusterConfig {
    /**
     * Host running the web server.
     */
    @Nullable
    public String webServer;
    /**
     * Hosts running the back-end workers.
     */
    @Nullable
    public String[] backends;
    /**
     * Folder where the hillview service is installed on each worker.
     */
    @Nullable
    public String serviceFolder;
    /**
     * User account that runs the hillview workers and web server.
     */
    @Nullable
    public String user;
    /**
     * Network port used by backend workers.
     */
    public int backendPort = -1;

    private void addField(String fieldName, ClusterConfigParser.ExpressionContext expression) {
        switch (fieldName) {
            case "webserver":
                this.webServer = evaluateToString(expression);
                break;
            case "backends":
                this.backends = evaluateToStringArray(expression);
                break;
            case "backend_port":
                this.backendPort = evaluateToInt(expression);
                break;
            case "user":
                this.user = evaluateToString(expression);
                break;
            case "service_folder":
                this.serviceFolder = evaluateToString(expression);
                break;
            default:
                // We ignore the other fields.
                break;
        }
    }

    private static String evaluateToString(ClusterConfigParser.ExpressionContext expression) {
        String s = expression.STRING().getText();
        if (s.startsWith("'''") || s.startsWith("\"\"\"")) {
            String prefix = s.substring(0, 3);
            if (!s.endsWith(prefix))
                throw new RuntimeException("Malformed string " + s);
            s = s.substring(3, s.length() - 3);
        } else {
            if (!s.endsWith(s.substring(0, 1)))
                throw new RuntimeException("Malformed string " + s);
            s = s.substring(1, s.length() - 1);
        }
        return StringEscapeUtils.unescapeJava(s);
    }

    private static int evaluateToInt(ClusterConfigParser.ExpressionContext expression) {
        return Integer.parseInt(expression.NUMBER().getText());
    }

    private static String[] evaluateToStringArray(ClusterConfigParser.ExpressionContext expression) {
        List<String> result = new ArrayList<String>();
        for (ClusterConfigParser.ExpressionContext expr: expression.array().expression()) {
            String s = evaluateToString(expr);
            result.add(s);
        }
        return result.toArray(new String[0]);
    }

    private void validate() {
        if (this.webServer == null)
            throw new RuntimeException("webserver not defined");
        if (this.backends == null)
            throw new RuntimeException("backends not defined");
        if (this.backendPort == -1)
            throw new RuntimeException("backend_port not defined");
        if (this.user == null)
            throw new RuntimeException("user not defined");
        // Other fields are not mandatory for now
    }

    /**
     * Parse a Python configuration file and create a Java
     * ClusterConfig object.  Python is a complicated language,
     * here we only support a small subset.
     */
    public static ClusterConfig parse(String file) throws IOException {
        ClusterConfig result = new ClusterConfig();
        CharStream stream = CharStreams.fromFileName(file);
        TokenSource tokenSource = new ClusterConfigLexer(stream);
        TokenStream inputTokenStream = new CommonTokenStream(tokenSource);
        ClusterConfigParser parser = new ClusterConfigParser(inputTokenStream);
        ClusterConfigParser.InitContext context = parser.init();
        List<ClusterConfigParser.AssignmentContext> assigns = context.assignment();
        for (ClusterConfigParser.AssignmentContext ctx : assigns)
            result.addField(ctx.IDENTIFIER().getText(), ctx.expression());
        result.validate();
        return result;
    }
}
