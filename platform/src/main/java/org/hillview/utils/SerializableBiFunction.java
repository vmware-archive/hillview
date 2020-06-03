package org.hillview.utils;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface SerializableBiFunction<I0, I1, O> extends BiFunction<I0, I1, O>, Serializable { }
