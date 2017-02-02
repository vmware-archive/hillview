package org.hiero.sketch.view;

import com.google.gson.Gson;

public interface IJson {
    final static Gson gsonInstance = new Gson();
    default String toJson() { return gsonInstance.toJson(this); }
}
