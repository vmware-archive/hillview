package org.hillview.table;

import javax.annotation.Nullable;

public class FilterPair implements IJsonRepr {
    // fields are never really null, but we have no default initializer
    @Nullable
    public FilterDescription first;
    @Nullable
    public FilterDescription second;
}
