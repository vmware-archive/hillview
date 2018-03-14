package org.hillview.table.membership;

import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ISampledRowIterator;

/**
 * Implements an ISampledRowIterator for the special case the rate is 1.
 */
public class NoSampleRowIterator implements ISampledRowIterator {
    private final IRowIterator iter;

    public NoSampleRowIterator(IRowIterator iter) {
        this.iter = iter;
    }

    @Override
    public double rate() { return 1; }

    @Override
    public int getNextRow() {
        return this.iter.getNextRow();
    }
}

