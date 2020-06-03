package org.hillview.sketches;

import org.hillview.utils.Randomness;

public class UniformSampleWorkspace {
    /**
     * Sampling rate used when adding an element.
     */
    private final double samplingRate;
    /**
     * Random number generator used during construction only.
     */
    private final Randomness random;
    /**
     * Insert a row only when skipRows is 0.
     */
    private int skipRows;

    public UniformSampleWorkspace(double samplingRate, long seed) {
        this.samplingRate = samplingRate;
        this.random = new Randomness(seed);
        this.skipRows = 0;
    }

    public boolean shouldSample() {
        boolean result = this.skipRows == 0;
        if (result)
            this.skipRows = this.random.nextGeometric(this.samplingRate);
        if (this.skipRows > 0)
            this.skipRows--;
        return result;
    }
}
