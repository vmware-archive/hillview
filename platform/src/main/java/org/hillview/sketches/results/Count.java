package org.hillview.sketches.results;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.hillview.dataset.api.IJsonSketchResult;

import java.util.Objects;

/**
 * This class represents the count in a histogram bucket.
 */
public class Count implements IJsonSketchResult {
    public long count;

    public Count() {
        this(0);
    }

    public Count(long l) {
        //this.checkSealed(this.sealed);
        this.count = l;
    }

    public void add(long l) {
        //this.checkSealed(this.sealed);
        this.count += l;
    }

    @Override
    public String toString() {
        return Long.toString(this.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Count count1 = (Count) o;
        return count == count1.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public JsonElement toJsonTree() { return new JsonPrimitive(this.count); }
}
