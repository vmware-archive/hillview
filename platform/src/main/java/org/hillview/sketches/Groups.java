package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.utils.JsonGroups;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;

import java.util.Objects;
import java.util.function.Function;

/**
 * Combine multiple sketch results into a "vector" of sketch results.
 * This is used to compute sketches that perform group-by using a set of
 * buckets.  This sketch result will have one result per bucket, plus
 * one result for "missing" data and one result for data that does not
 * fall into any bucket.
 * @param <R>  Type of sketch results that is grouped.
 */
public class Groups<R extends ISketchResult> implements ISketchResult {
    /**
     * For each bucket one result.
     */
    public final JsonList<R> perBucket;
    /**
     * For the bucket corresponding to the 'missing' value on result.
     */
    public final R           perMissing;
    /**
     * For the values that do not fall in any bucket.
     * Not yet useful, so commented-out.
    public final R           outOfRange;
     */

    public Groups(JsonList<R> perBucket, R perMissing /*, R outOfRange */) {
        this.perBucket = perBucket;
        this.perMissing = perMissing;
        //this.outOfRange = outOfRange;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < this.perBucket.size(); i++) {
            R ri = this.perBucket.get(i);
            result.append(i);
            result.append(" => ");
            result.append(ri.toString());
            result.append(System.lineSeparator());
        }
        result.append("missing => ");
        result.append(this.perMissing.toString());
        /*
        result.append(System.lineSeparator());
        result.append("out of range => ");
        result.append(this.outOfRange.toString());
        result.append(System.lineSeparator());
        */
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Groups<?> groups = (Groups<?>) o;
        return this.perBucket.equals(groups.perBucket) &&
                this.perMissing.equals(groups.perMissing) /* &&
                this.outOfRange.equals(groups.outOfRange) */;
    }

    @Override
    public int hashCode() {
        return Objects.hash(perBucket, perMissing /*, outOfRange */);
    }

    public <J extends IJson> JsonGroups<J> map(Function<R, J> map) {
        J missing = map.apply(this.perMissing);
        JsonList<J> perBucket = Linq.map(this.perBucket, map);
        return new JsonGroups<J>(perBucket, missing);
    }
}
