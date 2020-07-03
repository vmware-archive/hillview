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

package org.hillview.utils;

import org.hillview.dataset.api.IJson;

/**
 * This class represents a hostname and a port.
 */
@SuppressWarnings("CanBeFinal")
public class HostAndPort implements IJson {
    static final long serialVersionUID = 1;

    public String host;
    public int    port;

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static HostAndPort fromParts(String host, int port)  {
        return new HostAndPort(host, port);
    }

    public String getHost() { return this.host; }

    public int getPort() { return this.port; }

    public static HostAndPort fromString(String text) {
        int colon = text.indexOf(':');
        if (colon < 0)
            throw new RuntimeException("Invalid host and port: " + text + "; expected format is host:port");
        String host = text.substring(0, colon);
        String port = text.substring(colon + 1);
        int portNo = Integer.parseInt(port);
        return new HostAndPort(host, portNo);
    }

    public String toString() {
        return this.host + ":" + this.port;
    }
}
