package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ISubSchema;

public class FullSubSchema implements ISubSchema {
    @Override
    public boolean isColumnPresent(final String name) {
        return true;
    }
}
