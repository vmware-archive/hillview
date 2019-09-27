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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.PartialResult;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
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
     * A string describing the current stack trace.
     */
    public static String stackTraceString() {
        return ExceptionUtils.getStackTrace(new Throwable());
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
     * Converts an array of strings into a Map string->string.
     * The array must have an even number of elements, and
     * the elements are in order key-value pairs.
     */
    @Nullable
    public static HashMap<String, String> arrayToMap(@Nullable String[] array) {
        if (array == null)
            return null;
        HashMap<String, String> result = new HashMap<String, String>();
        if (array.length % 2 != 0)
            throw new IllegalArgumentException("Array must have an even number of elements");
        for (int index = 0; index < array.length; index += 2)
            result.put(array[index], array[index+1]);
        return result;
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
            HillviewLogger.instance.error("Cannot get host name", e);
            return e.getLocalizedMessage();
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
        // Apparently using an empty array is fast enough, we don't need
        // to pre-allocate the array.
        return list.toArray(new String[0]);
    }

    private static final List<String> compressionSuffixes = Arrays.asList(
            "gz", "bz", "bzip2", "xz", "Z", "arj");

    /**
     * Detect whether a file is compressed based on the file name.
     */
    public static boolean isCompressed(String filename) {
        String suffix = FilenameUtils.getExtension(filename).toLowerCase();
        return compressionSuffixes.contains(suffix);
    }

    /**
     * This function strips the path and the extension of a file name.
     * It also removes known compression suffixes.
     */
    public static String getBasename(String filename) {
        String basename = FilenameUtils.getBaseName(filename);
        if (isCompressed(basename))
            basename = FilenameUtils.removeExtension(basename);
        return FilenameUtils.removeExtension(basename);
    }

    public static String getWildcard(String path) {
        int last = path.lastIndexOf('/');
        last = Math.max(last, path.lastIndexOf('\\'));
        if (last == -1)
            return path;
        return path.substring(last + 1);
    }

    public static String getFolder(String path) {
        int last = path.lastIndexOf('/');
        last = Math.max(last, path.lastIndexOf('\\'));
        if (last == -1) {
            return "";
        }
        return path.substring(0, last);
    }

    /**
     * Trim character c from the beginning and end of string s.
     */
    public static String trim(String s, char c) {
        int skip = 0;
        int len = s.length();
        while (skip < len && s.charAt(skip) == c)
            skip++;
        int end = len - 1;
        while (end > skip && s.charAt(end) == c)
            end--;
        return s.substring(skip, end + 1);
    }

    /**
     * Convert consecutive multiple spaces into singlel spaces.
     */
    public static String singleSpaced(String s) {
        return s.replaceAll("^ +| +$|( )+", "$1");
    }

    /**
     * Given a string of the form ((k="v")[ ])*, extract the value associated with a specific key.
     * @param key Key to search.
     * @return The associated value or null.
     * TODO: this does not handle escaped quotes in the value.
     */
    @Nullable
    public static String getKV(@Nullable String s, String key) {
        if (s == null)
            return null;
        String[] parts = s.split(" ");
        for (String p : parts) {
            String[] kv = p.split("=");
            if (kv.length != 2)
                continue;
            if (kv[0].equals(key))
                return Utilities.trim(kv[1], '"');
        }
        return null;
    }

    /**
     * Returns the base-2 log of x, rounded down to the nearest long.
     */
    public static long longLog2(long x) {
        if (x <= 0) throw new RuntimeException("Attempted to take the log of a negative value: " + x);
        return (long)(Math.floor(Math.log(x) / Math.log(2)));
    }
}
