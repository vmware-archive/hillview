package org.hiero.sketch.table;

import java.util.ArrayList;
import java.util.BitSet;

/**
 * Base class for a column that can grow in size.
 */
abstract class BaseListColumn extends BaseColumn {
    final int LogSegmentSize = 20;
    final int SegmentSize = 1 << this.LogSegmentSize;
    final int SegmentMask = this.SegmentSize - 1;
    private boolean sealed;  // once sealed it can't grow anymore.

    private ArrayList<BitSet> missing;
    int size;

    BaseListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.allowMissing && !desc.kind.isObject())
            this.missing = new ArrayList<BitSet>();
        this.size = 0;
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        if (this.description.allowMissing) {
            if (this.missing == null)
                return false;
            final int segmentId = rowIndex >> this.LogSegmentSize;
            final int localIndex = rowIndex & this.SegmentMask;
            return this.missing.get(segmentId).get(localIndex);
        } else
            return false;
    }

    public void appendMissing() {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.missing.size() <= segmentId) {
            this.growMissing();
        }
        this.missing.get(segmentId).set(localIndex);
        this.size++;
    }

    void growMissing() {
        if (this.sealed)
            throw new RuntimeException("Cannot grow sealed column");
        if (this.missing != null)
            this.missing.add(new BitSet(this.SegmentSize));
    }

    public void seal() {
        this.sealed = true;
    }
}
