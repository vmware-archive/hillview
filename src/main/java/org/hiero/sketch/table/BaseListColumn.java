package org.hiero.sketch.table;

import java.util.ArrayList;
import java.util.BitSet;

/**
 * Base class for a column that can grow in size.
 */
abstract class BaseListColumn extends BaseColumn {
    final int LogSegmentSize = 20;
    final int SegmentSize = 1 << LogSegmentSize;
    final int SegmentMask = SegmentSize - 1;
    private boolean sealed;  // once sealed it can't grow anymore.

    protected ArrayList<BitSet> present;
    protected int size;

    BaseListColumn(ColumnDescription desc) {
        super(desc);
        if (desc.allowMissing)
            this.present = new ArrayList<BitSet>();
        this.size = 0;
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @Override
    public boolean isMissing(int rowIndex) {
        if (this.present == null)
            return false;
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        return this.present.get(segmentId).get(localIndex);
    }

    public void setMissing(int rowIndex) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        this.present.get(segmentId).set(localIndex);
    }

    public void seal() {
        this.sealed = true;
    }

    void growPresent() {
        if (this.sealed)
            throw new RuntimeException("Cannot grow sealed column");
        if (this.present != null)
            this.present.add(new BitSet(SegmentSize));
    }
}
