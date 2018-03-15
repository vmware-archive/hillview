package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import javax.annotation.Nullable;

public class NIThresholdSketch implements ISketch<ITable, NumItemsThreshold> {

        private final String colName;
        private final long seed; //seed for the hash function
        private final int logThreshold; //the log of the threshold size. The default is 13.

        public NIThresholdSketch(String colName, long seed) {
        this(colName, 13, seed);
    }

        public NIThresholdSketch(String colName, int logThreshold, long seed) {
        this.colName = colName;
        this.seed = seed;
        this.logThreshold = logThreshold;
    }

        @Override
        public NumItemsThreshold create(final ITable data) {
        NumItemsThreshold result = this.getZero();
        ColumnAndConverterDescription ccd = new ColumnAndConverterDescription(this.colName);
        ColumnAndConverter col = data.getLoadedColumn(ccd);
        result.createBits(col.column, data.getMembershipSet());
        return result;
    }

        @Override
        public NumItemsThreshold add(@Nullable final NumItemsThreshold left, @Nullable final NumItemsThreshold right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

        @Override
        public NumItemsThreshold zero() {
        return new NumItemsThreshold(this.logThreshold, this.seed);
    }
}
