package org.hillview.dataset;

import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.table.api.ITable;

public interface TableSketch<T extends ISketchResult> extends ISketch<ITable, T> {
}
