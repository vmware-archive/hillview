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

package org.hillview.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.PartialResult;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * This class has some useful static helper methods.
 */
public class Utilities {
    /**
     * Converts a throwable to a string.
     */
    @Nullable
    public static String throwableToString(@Nullable Throwable t) {
        if (t == null)
            return null;
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Gets the name of the local machine.
     */
    public static String getHostName() {
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            return localMachine.getHostName();
        } catch (java.net.UnknownHostException e) {
            HillviewLogger.instance.error("Cannot get host name");
            return "?";
        }
    }

    /**
     * PartialResult does not implement IJson, but some variants of it do.
     * @param pr  Partial result.
     * @param <T> Type of data in partial result.
     * @return    The JSON representation of the partial result.
     */
    public static <T extends IJson> JsonElement toJsonTree(PartialResult<T> pr) {
        JsonObject json = new JsonObject();
        json.addProperty("done", pr.deltaDone);
        if (pr.deltaValue == null)
            json.add("data", null);
        else
            json.add("data", pr.deltaValue.toJsonTree());
        return json;
    }

    public static <T> Iterable<T> arrayToIterable(T[] data) {
        return () -> new Iterator<T>() {
            private int pos = 0;

            public boolean hasNext() {
                return data.length > pos;
            }

            public T next() {
                return data[pos++];
            }

            public void remove() {
                throw new UnsupportedOperationException("Cannot remove an element of an array.");
            }
        };
    }
}
