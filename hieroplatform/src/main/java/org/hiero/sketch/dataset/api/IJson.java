package org.hiero.sketch.dataset.api;

import com.google.gson.Gson;

@SuppressWarnings("UnnecessaryInterfaceModifier")
public interface IJson {
    final static Gson gsonInstance = new Gson();
    default String toJson() { return gsonInstance.toJson(this); }
}
