/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview;

import org.hillview.utils.HillviewLogger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * This class manages configuration of the global root node of a Hillview deployment.
 */
public class Configuration {
    /**
     * This file contains the global properties that control hillview.
     * This file is read by the root node.
     */
    static final String propertiesFile = "hillview.properties";

    public static final Configuration instance = new Configuration();

    // Global application properties
    public final Properties properties;

    private Configuration() {
        this.properties = new Properties();
        try (FileInputStream prop = new FileInputStream(propertiesFile)) {
            this.properties.load(prop);
        } catch (FileNotFoundException ex) {
            HillviewLogger.instance.info("No properties file found", "{0}", propertiesFile);
        } catch (IOException ex) {
            HillviewLogger.instance.error("Error while loading properties from file", ex);
        }
    }

    public String getProperty(String propertyName, String defaultValue) {
        return this.properties.getProperty(propertyName, defaultValue);
    }

    public String getGreenplumDumpScript() {
        return this.getProperty(
                // The dump-greenplum.sh script will write its stdin to the specified file
                "greenplumDumpScript", "/home/gpadmin/hillview/dump-greenplum.sh");
    }

    public String getGreenplumDumpDirectory() {
        return this.getProperty("greenplumDumpDirectory", "/tmp");
    }

    public boolean getBooleanProperty(String prop) {
        String value = this.getProperty(prop, "false");
        return !value.trim().toLowerCase().equals("false");
    }
}
