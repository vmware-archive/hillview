package org.hiero.sketch.dataset.api;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

@SuppressWarnings("UnnecessaryInterfaceModifier")
public interface IJson {
    final static GsonBuilder builder = new GsonBuilder();
    final static Gson gsonInstance = builder.serializeNulls().create();

    /**
     * Default JSON string representation of this.
     * @return The JSON representation of this.
     */
    default String toJson() {
        JsonElement t = this.toJsonTree();
        return t.toString();
    }

    /**
     * Override this method if needed to implement the Json serialization.
     * @return The serialized representation of this.
     */
    default JsonElement toJsonTree() { return gsonInstance.toJsonTree(this); }
}
