package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.Converters;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.List;

public class RandomSamplingSketch implements ISketch<ITable, RandomSampling> {

    private final int numSamples;
    private final List<String> numericColNames;

    public RandomSamplingSketch(int numSamples, List<String> numericColNames) {
        this.numSamples = numSamples;
        this.numericColNames = numericColNames;
    }

    @Override
    public RandomSampling create(ITable data) {
        SmallTable subTable = data.compress(data.getMembershipSet().sample(numSamples));
        DoubleMatrix matrix = BlasConversions.toDoubleMatrix(subTable, numericColNames);
        return new RandomSampling(matrix);
    }

    @Override
    public RandomSampling zero() {
        return new RandomSampling(new DoubleMatrix(0, numericColNames.size()));
    }

    @Override
    public RandomSampling add(@Nullable RandomSampling left, @Nullable RandomSampling right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        return left.union(right, numSamples);
    }
}
