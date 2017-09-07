package org.hillview.table;

import javax.annotation.Nullable;

@SuppressWarnings("CanBeFinal")
public class ColTriple implements IJsonRepr {
    // fields are never really null, but we have no default initializer
    @Nullable
    public ColumnAndRange first;
    @Nullable
    public ColumnAndRange second;
    @Nullable
    public ColumnAndRange third;
}
