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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.JsonValue;

import java.io.File;
import java.util.Scanner;

/**
 * This class manages configuration of the global root node of a Hillview deployment.
 */
public class Configuration {
    /**
     * This file contains the global properties that control hillview.
     * This file is read by the root node.
     */
    static final String propertiesFile = "hillview.json";
    public static final Configuration instance = new Configuration();

    // Global application properties
    private final JsonObject properties;

    private Configuration() {
        JsonObject result = null;
        StringBuilder str = new StringBuilder();
        File file = new File(propertiesFile);
        try (Scanner prop = new Scanner(file)) {
            while (prop.hasNextLine()) {
                String line = prop.nextLine();
                if (line.matches("\\s+//.*"))
                    // Strip comments, not json conformant.
                    continue;
                str.append(line);
            }
            result = JsonParser.parseString(str.toString()).getAsJsonObject();
        } catch (Exception ex) {
            HillviewLogger.instance.error("Error while loading properties from file", ex);
        }
        if (result == null)
            result = new JsonObject();
        this.properties = result;
    }

    public JsonValue getAsJson() {
        return new JsonValue(this.properties);
    }

    public String getProperty(String propertyName, String defaultValue) {
        JsonElement el = this.properties.get(propertyName);
        if (el == null)
            return defaultValue;
        return el.getAsString();
    }

    public String getGreenplumMoveScript() {
        return this.getProperty(
                // The -greenplum.sh script will write its stdin to the specified file
                "greenplumMoveScript", "/home/gpadmin/hillview/move-greenplum.sh");
    }

    public String getGreenplumDumpDirectory() {
        return this.getProperty("greenplumDumpDirectory", "/tmp");
    }
}
