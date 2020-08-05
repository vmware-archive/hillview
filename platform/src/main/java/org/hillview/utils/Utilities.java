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
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.PartialResult;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;

/**
 * This class has some useful static helper methods.
 */
@SuppressWarnings("unused")
public class Utilities {
    public static final int INT_SIZE = 4;

    public static List<ITable> tail(List<ITable> data) {
        return data.subList(1, data.size());
    }

    @FunctionalInterface
    public
    interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);

        default <V> TriFunction<A, B, C, V> andThen(
                Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);
            return (A a, B b, C c) -> after.apply(apply(a, b, c));
        }
    }

    /**
     * Checks that an array is sorted.
     */
    public static <T extends Comparable<T>> void checkSorted(final T[] a) {
        for (int i = 0; i < (a.length - 1); i++)
            if (a[i].compareTo(a[i + 1]) >= 0)
                throw new IllegalArgumentException(a[i] + " and " + a[i+1] + " (index " + i +
                        ") are not in sorted order.");
    }

    public static double div(double value, long count) {
        if (count == 0)
            return value;  // probably also 0
        return value / count;
    }

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
                case '(': case ')': case '$':
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
     * Checks whether the local OS is Windows
     */
    public static boolean runningOnWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
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

    public static <T> T[] toArray(List<T> data, Class<T> clazz) {
        @SuppressWarnings("unchecked")
        T[] result = (T[]) Array.newInstance(clazz, data.size());
        for (int i = 0; i < data.size(); i++)
            result[i] = data.get(i);
        return result;
    }

    public static String[] toArray(List<String> list) {
        // Apparently using an empty array is fast enough, we don't need
        // to pre-allocate the array.
        return list.toArray(new String[0]);
    }

    private static final List<String> compressionSuffixes = Arrays.asList(
            "gz", "bz", "bzip2", "xz", "Z", "arj", "zip");

    /**
     * Detect whether a file is compressed based on the file name.
     * Returns the file extension if the file is compressed, null otherwise.
     */
    @Nullable
    public static String isCompressed(String filename) {
        String suffix = FilenameUtils.getExtension(filename).toLowerCase();
        if (compressionSuffixes.contains(suffix))
            return suffix;
        return null;
    }

    /**
     * This function strips the path and the extension of a file name.
     * It also removes known compression suffixes.
     */
    public static String getBasename(String filename) {
        String basename = FilenameUtils.getBaseName(filename);
        if (isCompressed(basename) != null)
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
     * Given a JSON string that represents an object return the
     * sub-object corresponding to the specified key.
     * @param json  JSON string.
     * @param key   Key name.
     * @return      The corresponding field, or null.
     */
    @Nullable
    public static String getJsonField(@Nullable String json, String key) {
        if (json == null)
            return null;
        Map<String, Object> map = IJson.gsonInstance.fromJson(json,
                new TypeToken<Map<String, String>>() {
                }.getType());
        if (map == null)
            return null;
        Object o = map.get(key);
        if (o == null)
            return null;
        return o.toString();
    }

    /**
     * Returns the base-2 log of x, rounded down to the nearest long.
     */
    public static int intLog2(int x) {
        if (x <= 0) {
            throw new RuntimeException("Attempted to take the log of a negative value: " + x);
        }
        return Converters.toInt((Math.floor(Math.log(x) / Math.log(2))));
    }

    /**
     * Returns the base-b log of x.
     */
    public static double logb(long x, int base) {
        if (x <= 0) {
            throw new RuntimeException("Attempted to take the log of a negative value: " + x);
        }
        return Math.log(x) / Math.log(base);
    }

    public static double stdev(ArrayList<Double> values) {
        double tot = 0;
        for ( double x : values ) {
            tot += x;
        }
        double mean = tot / values.size();
        double normsq = 0;
        for ( double x : values ) {
            normsq += Math.pow(x - mean, 2);
        }
        normsq /= values.size();
        return Math.sqrt(normsq);
    }

    /**
     * This function is given a comparison function as a string, like ==, <=.
     * The result is function that takes the result of a comparison (-1, 0, 1) and
     * returns 'true' when the result matches the comparison function.
     * @param operation  A string indicating a comparison.
     */
    public static Function<Integer, Boolean> convertComparison(String operation) {
        switch (operation) {
            case "==":
                return x -> x == 0;
            case "<=":
                return x -> x <= 0;
            case "<":
                return x -> x < 0;
            case ">":
                return x -> x > 0;
            case ">=":
                return x -> x >= 0;
            case "!=":
                return x -> x != 0;
            default:
                throw new HillviewException("Unexpected comparison: " + operation);
        }
    }

    public static <T> int indexOf(@Nullable T needle, T[] haystack) {
        for (int i = 0; i < haystack.length; i++) {
            if (needle == null) {
                if (haystack[i] == null)
                    return i;
            } else if (needle.equals(haystack[i])) {
                return i;
            }
        }
        return -1;
    }

    public static <T> List<T> emptyList() {
        return new ArrayList<T>();
    }

    @SafeVarargs
    public static <T> List<T> list(T... data) {
        ArrayList<T> a = new ArrayList<>();
        Collections.addAll(a, data);
        return a;
    }

    /**
     * Rescale an index from 0-maxBuckets to 0-totalElements.
     * Rank 0 is mapped to 0, rank maxBuckets is mapped to totalElements.
     * The other values of rank are interpolated in between.
     * @param rank            Rank of an element between 0 and maxBuckets.
     * @param maxBuckets      Number of ranks desired.
     * @param totalElements   Number of elements from which we extract the rank.
     */
    public static int getIntegerRank(int rank, int maxBuckets, int totalElements) {
        return (int) Math.ceil(totalElements * rank / ((double)maxBuckets));
    }

    /**
     * Extract `count` equi-spaced elements from `data`
     * @param data    Data to extract from.
     * @param count   Number of values to extract.  If data has fewer values return all.
     * @param result  Deposit the result here.
     */
    public static <T> void equiSpaced(List<T> data, int count, List<T> result) {
        int size = data.size();
        if (size <= count) {
            result.addAll(data);
        } else {
            for (int i = 0; i < count; i++) {
                int j = Utilities.getIntegerRank(i, count, size);
                result.add(data.get(j));
            }
        }
    }

    public static void intToByteArray(Integer p, /*out*/byte[] arr, int startIndex) {
        for (int i = INT_SIZE - 1; i >= 0; i--) {
            arr[i + startIndex] = (byte)(p >> 8*i);
        }
    }

    /* assumes arr has 2*INT_SIZE space allocated */
    public static void intPairToByteArray(Pair<Integer, Integer> p, /*out*/byte[] arr, int startIndex) {
        intToByteArray(Converters.checkNull(p.first), arr, startIndex);
        intToByteArray(Converters.checkNull(p.second), arr, startIndex + INT_SIZE);
    }

    public static long byteArrayToLong(byte[] bytes) {
        if (bytes.length < 8) {
            throw new RuntimeException("Not enough bytes to convert to int");
        }

        long value = 0L;
        for (int i = 0; i < 7; i++) {
            value += ((long) bytes[i] & 0xffL) << (8 * i);
        }
        value += (((long) bytes[7] & 0xffL) << 48);
        value &=  ((1L << 53L) - 1L);
        return value;
    }

    /**
     * Keep every k-th element of an array.
     */
    public static int[] decimate(int[] data, int k) {
        int[] copy = new int[(data.length + k - 1)/ k];
        for (int i = 0; i < data.length; i += k)
            copy[i / k] = data[i];
        return copy;
    }

    /**
     * Keep every k-th element of an array.
     */
    public static double[] decimate(double[] data, int k) {
        double[] copy = new double[(data.length + k - 1)/ k];
        for (int i = 0; i < data.length; i += k)
            copy[i / k] = data[i];
        return copy;
    }

    /**
     * Keep every k-th element of a list.
     */
    public static <T> List<T> decimate(List<T> data, int k) {
        List<T> copy = new ArrayList<T>((data.size() + k - 1)/ k);
        for (int i = 0; i < data.size(); i += k)
            copy.add(data.get(i));
        return copy;
    }

    /**
     * Keep every k-th element of an array.
     */
    public static String[] decimate(String[] data, int k) {
        String[] copy = new String[data.length / k];
        for (int i = 0; i < data.length; i += k)
            copy[i / k] = data[i];
        return copy;
    }
}
