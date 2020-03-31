package org.hillview.sketches.results;

import org.hillview.utils.Linq;

import java.io.Serializable;

/**
 * A quantiles vector is a vector - with one element for each bucket of a
 * histogram - of quantiles.
 */
public class QuantilesVector implements Serializable {
    static final long serialVersionUID = 1;
    public final NumericSamples[] data;

    public QuantilesVector(NumericSamples[] data) {
        this.data = data;
    }

    public int size() {
        return this.data.length;
    }

    public QuantilesVector add(QuantilesVector other) {
        if (this.size() != other.size())
            throw new RuntimeException("Incompatible sizes: " + this.size() + " and " + other.size());
        NumericSamples[] result = new NumericSamples[this.size()];
        for (int i = 0; i < this.size(); i++) {
            result[i] = this.data[i].add(other.data[i]);
        }
        return new QuantilesVector(result);
    }

    public void seal() {
        for (NumericSamples datum : this.data) datum.seal();
    }

    public void add(int bucket, double d) {
        this.data[bucket].add(d);
    }

    public QuantilesVector quantiles(int expectedCount) {
        NumericSamples[] result = Linq.map(this.data, n -> n.quantiles(expectedCount), NumericSamples.class);
        return new QuantilesVector(result);
    }
}