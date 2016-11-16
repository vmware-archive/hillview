package org.hiero.sketch.table;
import org.hiero.sketch.table.api.ISubSchema;
import java.util.HashSet;

public class HashSubSchema implements ISubSchema {
    private HashSet<String> colNames;

    public HashSubSchema() {
        colNames = new HashSet<String>();
    }

    public void add(String newCol){
        colNames.add(newCol);
    }

    @Override
    public boolean isColumnPresent(String name) {
        return colNames.contains(name);
    }
}
