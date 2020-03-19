package org.hillview.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.hillview.dataset.api.IJson;

/**
 * An integer which can be serialized as Json.
 */
public class JsonInteger implements IJson {
    static final long serialVersionUID = 1;

    private final int value;

    public JsonInteger(int value) {
        this.value = value;
    }

    @Override
    public JsonElement toJsonTree() {
        return new JsonPrimitive(value);
    }
}
