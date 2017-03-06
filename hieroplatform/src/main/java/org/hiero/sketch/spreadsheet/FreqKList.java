package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.SmallTable;

import java.io.Serializable;
import java.util.List;

public class FreqKList implements Serializable, IJson {
    public final SmallTable table;
    public final List<Integer> count;
}
