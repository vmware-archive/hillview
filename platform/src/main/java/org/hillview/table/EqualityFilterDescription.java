package org.hillview.table;

import org.hillview.dataset.api.IJson;

import java.io.Serializable;

public class EqualityFilterDescription implements Serializable, IJson {
    public ColumnDescription columnDescription;
    public String compareValue;
    public boolean complement;

    public EqualityFilterDescription(ColumnDescription columnDescription, String compareValue, boolean complement) {
        this.columnDescription = columnDescription;
        this.compareValue = compareValue;
        this.complement = complement;
    }
}
