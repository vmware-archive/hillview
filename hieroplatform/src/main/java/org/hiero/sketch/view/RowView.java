package org.hiero.sketch.view;

public class RowView implements IJson {
    private final int count;
    private final Object[] values;

    public RowView(int count, Object[] values) {
        this.count = count;
        this.values = values;
    }
}
