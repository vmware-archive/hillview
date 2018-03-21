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
import java.util.List;

/**
 * This class has some useful static helper methods.
 */
public class Utilities {
    /**
     * Convert a shell-type filename wildcard pattern into a Java
     * regular expression string.
     * @param wildcard  Simple filename wildcard string.
     * @return          A String that represents a regular expression
     *                  with the same semantics as the wildcard pattern.
     */
    @Nullable
    public static String wildcardToRegex(@Nullable String wildcard) {
        if (wildcard == null)
            return null;
        StringBuilder s = new StringBuilder(wildcard.length());
        s.append('^');
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch(c) {
                case '*':
                    s.append(".*");
                    break;
                case '?':
                    s.append(".");
                    break;
                case '(': case ')': case '[': case ']': case '$':
                case '.': case '{': case '}': case '|':
                case '\\':
                    s.append("\\");
                    s.append(c);
                    break;
                default:
                    s.append(c);
                    break;
            }
        }
        s.append('$');
        return(s.toString());
    }

    /**
     * Return a prefix of the argument if it is too long.
     */
    @Nullable
    public static String truncateString(@Nullable String s) {
        if (s == null || s.length() < 100)
            return s;
        return s.substring(0, 99) + "...";
    }

    /**
     * Checks if the argument string is null or empty.
     */
    public static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Returns null if the argument is null or empty.
     */
    @Nullable
    public static String nullIfEmpty(@Nullable String s) {
        if (s == null || s.isEmpty())
            return null;
        return s;
    }

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

    public static String[] toArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }
}
