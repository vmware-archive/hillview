package org.hiero.sketch.table;
import org.hiero.sketch.table.api.ISubSchema;
import java.util.HashSet;

public class HashSubSchema implements ISubSchema {
    private final HashSet<String> colNames;

    public HashSubSchema() {
        this.colNames = new HashSet<String>();
    }

    public void add(final String newCol){
        this.colNames.add(newCol);
    }

    @Override
    public boolean isColumnPresent(final String name) {
        return this.colNames.contains(name);
    }
}
