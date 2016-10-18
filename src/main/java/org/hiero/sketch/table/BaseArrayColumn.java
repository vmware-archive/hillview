package org.hiero.sketch.table;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * If description allows missing values, adds a BitSet missing to BaseColumn to evaluate IsMissing.
 */
public abstract class BaseArrayColumn extends BaseColumn {

    protected BitSet missing;


    protected BaseArrayColumn(ColumnDescription description, int size) {
        super(description);
        if (size <= 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        else if (this.description.allowMissing)
            missing = new BitSet(size);
    }

    protected BaseArrayColumn(ColumnDescription description, BitSet missing) {
        super(description);
        if (this.description.allowMissing)
            this.missing = missing;
        else
            throw new InvalidParameterException("Description does not allow missing values");
    }

    @Override
    public boolean isMissing(int rowIndex) { return missing.get(rowIndex); }

    /* Set methods from Bitset class*/
    public void setMissing(int rowIndex) { this.missing.set(rowIndex); }

    public void setMissing(int rowIndex, boolean val) { this.missing.set(rowIndex, val); }

    public void setMissing(int fromIndex, int toIndex){ this.missing.set(fromIndex, toIndex); }

    public void setMissing(int fromIndex, int toIndex, boolean val){this.missing.set(fromIndex, toIndex, val); }

}
