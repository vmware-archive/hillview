package org.hillview.table.api;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.io.Serializable;

public class ColumnNameAndConverter implements Serializable, IJson {
    public final String columnName;
    @Nullable
    public final IStringConverter converter;

    public ColumnNameAndConverter(String columnName, @Nullable IStringConverter converter) {
        this.columnName = columnName;
        this.converter = converter;
    }

    public ColumnNameAndConverter(String columnName) {
        this(columnName, null);
    }
}
