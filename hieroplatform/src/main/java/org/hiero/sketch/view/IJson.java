package org.hiero.sketch.view;

import com.google.gson.Gson;

public interface IJson {
    Gson gsonInstance = new Gson();
    default String toJson() { return gsonInstance.toJson(this); }
}
