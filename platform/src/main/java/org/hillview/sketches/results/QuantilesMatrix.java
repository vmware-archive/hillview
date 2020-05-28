package org.hillview.sketches.results;

import org.hillview.dataset.api.IJson;
import org.hillview.utils.Linq;

/**
 * A quantiles vector is a vector - with one element for each bucket of a
 * histogram - of quantiles.
 */
public class QuantilesMatrix implements IJson {
    static final long serialVersionUID = 1;
    public final QuantilesVector[] data;
    long outOfBounds;

    public QuantilesMatrix(QuantilesVector[] data, long outOfBounds) {
        this.data = data;
        this.outOfBounds = outOfBounds;
    }

    public static QuantilesMatrix zero(double[][] samplingRates, long seed) {
        QuantilesVector[] result = new QuantilesVector[samplingRates.length];
        for (int i = 0; i < samplingRates.length; i++) {
            result[i] = QuantilesVector.zero(samplingRates[i], seed + i);
        }
        return new QuantilesMatrix(result, 0);
    }

    public int size() {
        return this.data.length;
    }

    public QuantilesMatrix add(QuantilesMatrix other) {
        if (this.size() != other.size())
            throw new RuntimeException("Incompatible sizes: " +
                    this.size() + " and " + other.size());
        QuantilesVector[] result = new QuantilesVector[this.size()];
        for (int i = 0; i < this.size(); i++)
            result[i] = this.data[i].add(other.data[i]);
        return new QuantilesMatrix(result, this.outOfBounds + other.outOfBounds);
    }

    public void seal() {
        for (QuantilesVector datum : this.data)
            datum.seal();
    }

    public void add(int xBucket, int yBucket, double d) {
        this.data[xBucket].add(yBucket, d);
    }

    public QuantilesMatrix quantiles(int expectedCount) {
        QuantilesVector[] result = Linq.map(this.data, n -> n.quantiles(expectedCount), QuantilesVector.class);
        return new QuantilesMatrix(result, this.outOfBounds);
    }

    public void addMissing(int xBucket, int yBucket) {
        this.data[xBucket].addMissing(yBucket);
    }

    public void outOfBounds() {
        this.outOfBounds++;
    }
}