package org.hillview.sketches;

import org.hillview.utils.MetricMDS;
import org.jblas.DoubleMatrix;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSampling implements Serializable {

    public final DoubleMatrix data;

    public RandomSampling(DoubleMatrix data) {
        this.data = data;
    }

    public RandomSampling union(RandomSampling right, int numSamples) {
        List<Integer> sampleIndices = IntStream.range(0, this.data.rows + right.data.rows).boxed().collect(Collectors.toList());
        Collections.shuffle(sampleIndices);
        sampleIndices = sampleIndices.subList(0, Math.min(numSamples, sampleIndices.size()));
        int numColumns = this.data.columns > 0 ? this.data.columns : right.data.columns;
        DoubleMatrix result = new DoubleMatrix(sampleIndices.size(), this.data.columns);
        int i = 0;
        for (Integer sampleIndex : sampleIndices) {
            DoubleMatrix sample;
            if (sampleIndex >= this.data.rows) {
                sample = right.data.getRow(sampleIndex - this.data.rows);
            } else {
                sample = this.data.getRow(sampleIndex);
            }
            result.putRow(i++, sample);
        }
        return new RandomSampling(result);
    }
}
