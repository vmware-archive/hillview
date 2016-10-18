package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipMap;
import org.hiero.sketch.table.api.ISchema;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table {
    private ISchema schema;
    private IColumn[] columns;
    private IMembershipMap members;
    // TODO
}
