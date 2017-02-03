package org.hiero.sketch.view;

import org.hiero.sketch.dataset.api.IJson;

@SuppressWarnings("FieldCanBeLocal")
public class RowView implements IJson {
    private final int count;
    private final Object[] values;

    public RowView(int count, Object[] values) {
        this.count = count;
        this.values = values;
    }
}
