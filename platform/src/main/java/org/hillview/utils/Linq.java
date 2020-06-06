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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Some utility classes inspired by C# Linq.
 */
public class Linq {
    static class MapIterator<T, S> implements Iterator<S> {
        final Iterator<T> data;
        final Function<T, S> map;

        MapIterator(Iterator<T> data, Function<T, S> function) {
            this.data = data;
            this.map = function;
        }

        @Override
        public boolean hasNext() {
            return this.data.hasNext();
        }

        public S next() {
            T next = this.data.next();
            return this.map.apply(next);
        }
    }

    static class MapIterable<T, S> implements Iterable<S> {
        final MapIterator<T, S> mapIterator;

        MapIterable(Iterable<T> data, Function<T, S> function) {
            this.mapIterator = new MapIterator<T, S>(data.iterator(), function);
        }

        @Override
        public Iterator<S> iterator() {
            return this.mapIterator;
        }
    }

    public static <T, S> Iterable<S> map(Iterable<T> data, Function<T, S> function) {
        return new MapIterable<T, S>(data, function);
    }

    public static <T, S> Iterator<S> map(Iterator<T> data, Function<T, S> function) {
        return new MapIterator<T, S>(data, function);
    }

    public static <T, S> JsonList<S> map(List<T> data, Function<T, S> function) {
        JsonList<S> result = new JsonList<S>(data.size());
        for (T aData : data)
            result.add(function.apply(aData));
        return result;
    }

    public static <T> ArrayList<T> where(List<T> data, Predicate<T> function) {
        ArrayList<T> result = new ArrayList<T>();
        for (T aData : data)
            if (function.test(aData))
                result.add(aData);
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] where(T[] data, Predicate<T> function) {
        List<T> result = new ArrayList<T>();
        for (T datum : data)
            if (function.test(datum))
                result.add(datum);
        return (T[]) result.toArray();
    }

    public static <T> boolean any(Iterable<T> data, Predicate<T> test) {
        for (T d: data)
            if (test.test(d)) {
                return true;
            }
        return false;
    }

    public static <T, S> S[] map(T[] data, Function<T, S> function, Class<S> sc) {
        @SuppressWarnings("unchecked")
        S[] result = (S[])Array.newInstance(sc, data.length);
        for (int i=0; i < data.length; i++)
            result[i] = function.apply(data[i]);
        return result;
    }

    public static <T, S> JsonList<S> mapToList(T[] data, Function<T, S> function) {
        JsonList<S> result = new JsonList<S>(data.length);
        for (int i=0; i < data.length; i++)
            result.add(function.apply(data[i]));
        return result;
    }

    public static <T, S> Pair<T, S>[] zip(T[] l, S[] r, Class<Pair<T, S>> sc) {
        if (l.length != r.length)
            throw new RuntimeException("Zip vectors with uneven lengths: " +
                    l.length + " and " + r.length);
        @SuppressWarnings("unchecked")
        Pair<T, S>[] result = (Pair<T, S>[])Array.newInstance(sc, l.length);
        for (int i=0; i < l.length; i++)
            result[i] = new Pair<T, S>(l[i], r[i]);
        return result;
    }

    public static <T, S> ArrayList<Pair<T, S>> zip(List<T> l, List<S> r) {
        if (l.size() != r.size())
            throw new RuntimeException("Zip lists with uneven lengths: " +
                    l.size() + " and " + r.size());
        ArrayList<Pair<T, S>> result = new ArrayList<Pair<T, S>>(l.size());
        for (int i=0; i < l.size(); i++)
            result.add(new Pair<T, S>(l.get(i), r.get(i)));
        return result;
    }

    public static <T, S, R> JsonList<R> zipMap(List<T> l, List<S> r, BiFunction<T, S, R> f) {
        if (l.size() != r.size())
            throw new RuntimeException("Zip lists with uneven lengths: " +
                    l.size() + " and " + r.size());
        JsonList<R> result = new JsonList<R>(l.size());
        for (int i=0; i < l.size(); i++)
            result.add(f.apply(l.get(i), r.get(i)));
        return result;
    }

    public static <T, S, R> JsonList<R> zipMap(List<T> l, List<S> r, List<BiFunction<T, S, R>> f) {
        if (l.size() != r.size())
            throw new RuntimeException("Zip lists with uneven lengths: " +
                    l.size() + " and " + r.size());
        JsonList<R> result = new JsonList<R>(l.size());
        for (int i=0; i < l.size(); i++)
            result.add(f.get(i).apply(l.get(i), r.get(i)));
        return result;
    }

    public static <T, S, R> Triple<T, S, R>[] zip3(T[] t, S[] s, R[] r, Class<Triple<T, S, R>> sc) {
        if (t.length != s.length || s.length != r.length)
            throw new RuntimeException("Zip3 vectors with uneven lengths: " +
                    t.length + ", " + s.length + ", and " + r.length);
        @SuppressWarnings("unchecked")
        Triple<T, S, R>[] result = (Triple<T, S, R>[])Array.newInstance(sc, t.length);
        for (int i=0; i < t.length; i++)
            result[i] = new Triple<T, S, R>(t[i], s[i], r[i]);
        return result;
    }

    public static <T, S, R> ArrayList<Triple<T, S, R>> zip3(
            List<T> t, List<S> s, List<R> r) {
        if (t.size()  != s.size()  || s.size()  != r.size() )
            throw new RuntimeException("Zip3 vectors with uneven lengths: " +
                    t.size()  + ", " + s.size()  + ", and " + r.size() );
        ArrayList<Triple<T, S, R>> result = new ArrayList<Triple<T, S, R>>(t.size());
        for (int i=0; i < t.size() ; i++)
            result.add(new Triple<T, S, R>(t.get(i), s.get(i), r.get(i)));
        return result;
    }
}
