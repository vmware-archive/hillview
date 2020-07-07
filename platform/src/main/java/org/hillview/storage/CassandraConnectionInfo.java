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
 */

package org.hillview.storage;

import java.io.Serializable;
import javax.annotation.Nullable;

public class CassandraConnectionInfo implements Serializable {
    static final long serialVersionUID = 1;

    public String host;
    // Port for establising probe connection
    public String jmxPort;
    // Port for opening client connection
    public String nativePort;
    public String cassandraRootDir;
    @Nullable
    public String username;
    @Nullable
    public String password;

    public CassandraConnectionInfo(String cassandraRootDir, String host, String jmxPort, String nativePort, String... auth){
        this.cassandraRootDir = cassandraRootDir;
        this.host = host;
        this.jmxPort = jmxPort;
        this.nativePort = nativePort;
        if (auth.length == 2){
            this.username = auth[0];
            this.password = auth[1];
        }
    }

    @Override
    public String toString() {
        return this.cassandraRootDir + "/" + this.nativePort;
    }
}
