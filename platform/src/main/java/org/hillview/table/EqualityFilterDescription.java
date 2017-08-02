package org.hillview.table;

import org.hillview.dataset.api.IJson;

import java.io.Serializable;

public class EqualityFilterDescription implements Serializable, IJson {
    public ColumnDescription columnDescription;
    public String compareValue;
    public boolean complement;
}
