package org.hillview.sketches.results;

import org.hillview.dataset.api.IJson;
import org.hillview.utils.JsonList;
import org.hillview.utils.Randomness;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Sample values from a numeric distribution, with the two extrema.
 */
public class NumericSamples implements IJson {
    static final long serialVersionUID = 1;

    /**
     * If true the set of samples is empty.
     */
    public boolean empty;
    /**
     * Minimum value in distribution.
     */
    public double min;
    /**
     * Maximum value in distribution.
     */
    public double max;
    /**
     * A list of samples computed by sampling with the
     * specified rate.
     */
    public JsonList<Double> samples;
    /**
     * Number of missing values.
     */
    public long missing;

    // Following 2 are only used only during construction.
    /**
     * Sampling rate used when adding an element.
     */
    private final double samplingRate;
    /**
     * Random number generator used during construction only.
     */
    @Nullable
    private Randomness random;

    public NumericSamples(double samplingRate, long seed) {
        this.samplingRate = samplingRate;
        this.missing = 0;
        this.random = new Randomness(seed);
        this.empty = true;
        this.samples = new JsonList<Double>();
    }

    private NumericSamples(List<Double> data) {
        this.empty = false;
        this.missing = 0;
        this.samplingRate = 0;
        this.samples = new JsonList<Double>(data);
    }

    public int size() {
        return this.samples.size();
    }

    public boolean empty() {
        return this.empty;
    }

    /**
     * Combine two sets of numeric samples.  It just concatenates the lists.
     */
    NumericSamples add(NumericSamples other) {
        NumericSamples result = new NumericSamples(this.samples);
        if (this.empty()) {
            result.min = other.min;
            result.max = other.max;
            result.empty = other.empty;
        } else if (other.empty()) {
            result.min = this.min;
            result.max = this.max;
        } else {
            result.min = Math.min(this.min, other.min);
            result.max = Math.max(this.max, other.max);
        }
        result.missing = this.missing + other.missing;
        result.samples.addAll(other.samples);
        return result;
    }

    /**
     * Here is a number from the distribution; sample it.
     */
    public void add(double d) {
        if (this.empty) {
            this.empty = false;
            this.min = d;
            this.max = d;
        } else {
            this.min = Math.min(this.min, d);
            this.max = Math.max(this.max, d);
        }
        assert this.random != null;
        if (this.random.nextDouble() <= this.samplingRate)
            this.samples.add(d);
    }

    /**
     * We are done adding data.
     */
    public void seal() {
        this.random = null;
    }

    /**
     * Extract only the specified number of quantiles from the NumericSamples.
     * @param expectedCount Number of empirical quantiles to extract.
     */
    public NumericSamples quantiles(int expectedCount) {
        Collections.sort(this.samples);
        if (this.empty || this.samples.size() < expectedCount)
            return this;
        List<Double> small = Utilities.decimate(this.samples, Math.floorDiv(this.samples.size(), expectedCount));
        NumericSamples result = new NumericSamples(small);
        result.min = this.min;
        result.max = this.max;
        return result;
    }

    public void addMissing() {
        this.missing++;
    }
}
