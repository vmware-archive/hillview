package org.hillview.table;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.io.Serializable;

public class FilterDescription implements Serializable, IJson {
    public String columnName = "";
    public double min;
    public double max;
    public boolean complement;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns
}
