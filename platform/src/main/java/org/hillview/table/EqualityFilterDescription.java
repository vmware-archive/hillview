package org.hillview.table;

import org.hillview.dataset.api.IJson;

import java.io.Serializable;

public class EqualityFilterDescription implements Serializable, IJson {
    public final ColumnDescription columnDescription;
    public final String compareValue;
    public final boolean complement;

    public EqualityFilterDescription(ColumnDescription columnDescription, String compareValue, boolean complement) {
        this.columnDescription = columnDescription;
        this.compareValue = compareValue;
        this.complement = complement;
    }
}
