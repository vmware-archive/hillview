package org.hillview.table;

import org.hillview.dataset.api.IJson;

import java.io.Serializable;

public class EqualityFilterDescription implements Serializable, IJson {
    public String columnName;
    public Object compareValue;
    public boolean complement;
}
