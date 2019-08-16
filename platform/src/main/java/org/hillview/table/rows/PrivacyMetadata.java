package org.hillview.table.rows;

import org.hillview.dataset.api.IJson;

public class PrivacyMetadata implements IJson {
    public double epsilon;
    public double granularity;

    public double globalMin;
    public double globalMax;

    public PrivacyMetadata(double epsilon, double granularity,
                           double globalMin, double globalMax) {
        this.epsilon = epsilon;
        this.granularity = granularity;
        this.globalMin = globalMin;
        this.globalMax = globalMax;
    }
}
