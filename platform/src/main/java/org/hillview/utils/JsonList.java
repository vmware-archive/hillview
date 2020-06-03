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

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.Pair;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Wrapper around a list which makes it serializable as JSON.
 */
public class JsonList<T> implements List<T>, IJsonSketchResult {
    static final long serialVersionUID = 1;

    private final List<T> data;

    public JsonList() {
        this.data = new ArrayList<T>();
    }

    public JsonList(int size) {
        this.data = new ArrayList<T>(size);
    }

    public JsonList(List<T> data) {
        this.data = data;
    }

    public JsonList(@Nullable T value) {
        this(1);
        this.add(value);
    }

    public JsonList(@Nullable T v1, @Nullable T v2) {
        this(2);
        this.add(v1);
        this.add(v2);
    }

    public JsonList(@Nullable T v1, @Nullable T v2, @Nullable T v3) {
        this(3);
        this.add(v1);
        this.add(v2);
        this.add(v3);
    }

    @SuppressWarnings({"ManualArrayToCollectionCopy", "UseBulkOperation"})
    public JsonList(T[] data) {
        this(data.length);
        for (T t : data)
            this.add(t);
    }

    @Override
    public int size() {
        return this.data.size();
    }

    @Override
    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.data.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return this.data.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.data.toArray();
    }

    @SuppressWarnings("SuspiciousToArrayCall")
    @Override
    public <T1> T1[] toArray(T1[] a) {
        return this.data.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return this.data.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return this.data.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.data.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return this.data.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        return this.data.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.data.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.data.retainAll(c);
    }

    @Override
    public void clear() {
        this.data.clear();
    }

    @Override
    public T get(int index) {
        return this.data.get(index);
    }

    @Override
    public T set(int index, T element) {
        return this.data.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        this.data.add(index, element);
    }

    @Override
    public T remove(int index) {
        return this.data.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.data.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.data.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return this.data.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return this.data.listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return new JsonList<T>(this.data.subList(fromIndex, toIndex));
    }

    public <S> JsonList<Pair<T, S>> zip(JsonList<S> other) {
        int len = Math.min(this.size(), other.size());
        JsonList<Pair<T, S>> result = new JsonList<Pair<T, S>>(len);
        for (int i=0; i < len; i++)
            result.add(new Pair<T, S>(this.get(i), other.get(i)));
        return result;
    }

    public <S, U>
    JsonList<U> zip(JsonList<S> other, BiFunction<T, S, U> func) {
        int len = Math.min(this.size(), other.size());
        JsonList<U> result = new JsonList<U>(len);
        for (int i=0; i < len; i++)
            result.add(func.apply(this.get(i), other.get(i)));
        return result;
    }

    public <S> JsonList<S> map(Function<T, S> func) {
        JsonList<S> result = new JsonList<S>(this.size());
        for (int i=0; i < this.size(); i++)
            result.add(func.apply(this.get(i)));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonList<?> jsonList = (JsonList<?>) o;
        return data.equals(jsonList.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() { return this.data.toString(); }
}
