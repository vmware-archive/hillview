package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.SmallTable;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.Converters;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This sketch selects random rows from the table, and gives as a result a DoubleMatrix. The rows in the matrix are
 * the randomly selected rows, and the columns are the numeric columns specified in the constructor. The number of
 * samples is specified in the constructor too.
 *
 * Currently, only non-missing columns are supported.
 */
public class RandomSamplingSketch implements ISketch<ITable, RandomSampling> {

    private final int numSamples;
    private final List<String> columnNames;
    private final boolean allowMissing;

    public RandomSamplingSketch(int numSamples, List<String> columnNames, boolean allowMissing) {
        this.numSamples = numSamples;
        this.columnNames = columnNames;
        this.allowMissing = allowMissing;
    }

    @Override
    public RandomSampling create(ITable data) {
        SmallTable sample;
        if (this.allowMissing) {
            sample = data.compress(data.getMembershipSet().sample(numSamples));
        } else {
            List<IColumn> columns = this.columnNames.stream().map(data::getColumn).collect(Collectors.toList());
            sample = data.compress(data.getMembershipSet().filter((row) -> {
                for (IColumn column : columns) {
                    if (column.isMissing(row))
                        return false;
                }
                return true;
            }).sample(numSamples));
        }
        return new RandomSampling(sample, this.numSamples, data.getMembershipSet().getSize());
    }

    @Override
    public RandomSampling zero() {
        return new RandomSampling(new SmallTable(), this.numSamples, 0);
    }

    @Override
    public RandomSampling add(@Nullable RandomSampling left, @Nullable RandomSampling right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        return left.union(right);
    }
}
