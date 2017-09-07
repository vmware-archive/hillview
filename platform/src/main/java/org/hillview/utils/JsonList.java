package org.hillview.utils;

import org.hillview.dataset.api.IJson;

import java.util.*;

/**
 * Wrapper around a list which makes it serializable as JSON.
 */
public class JsonList<T> implements IJson, List<T> {
    final List<T> data;

    public JsonList(int size) {
        this.data = new ArrayList<T>(size);
    }

    public JsonList(List<T> data) {
        this.data = data;
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
}
