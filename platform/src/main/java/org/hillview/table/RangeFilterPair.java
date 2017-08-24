package org.hillview.table;

import javax.annotation.Nullable;

@SuppressWarnings("CanBeFinal")
public class RangeFilterPair implements IJsonRepr {
    // fields are never really null, but we have no default initializer
    @Nullable
    public RangeFilterDescription first;
    @Nullable
    public RangeFilterDescription second;
}
