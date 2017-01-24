package org.hiero.sketch.view;

public class RowView implements IJson {
    public final int count;
    public final Object[] values;

    public RowView(int count, Object[] values) {
        this.count = count;
        this.values = values;
    }
}
