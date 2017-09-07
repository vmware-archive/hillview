package org.hillview.table.api;

import java.util.function.Consumer;

public interface ICategoryColumn extends IStringColumn {
    /**
     * Computes all distinct values in the column.
     * @param action: Action invoked for each distinct string.
     */
    void allDistinctStrings(Consumer<String> action);
}
