package org.hillview.table;

import javax.annotation.Nullable;

@SuppressWarnings("CanBeFinal")
public class ColPair implements IJsonRepr {
    @Nullable
    public ColumnAndRange first;
    @Nullable
    public ColumnAndRange second;
}
