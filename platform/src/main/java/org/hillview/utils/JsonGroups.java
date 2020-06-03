package org.hillview.utils;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketchResult;

import java.util.Objects;

/**
 * This is a version of Group which is serializable as JSON.
 * @param <R>  Type of data that is grouped.
 */
public class JsonGroups<R extends IJson> implements IJson {
    /**
     * For each bucket one result.
     */
    public final JsonList<R> perBucket;
    /**
     * For the bucket corresponding to the 'missing' value on result.
     */
    public final R           perMissing;

    public JsonGroups(JsonList<R> perBucket, R perMissing) {
        this.perBucket = perBucket;
        this.perMissing = perMissing;
    }
}
