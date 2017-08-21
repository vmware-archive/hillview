package org.hillview;

import org.hillview.sketches.CorrMatrix;

public class CorrelationMatrixTarget extends RpcTarget {
    final CorrMatrix corrMatrix;

    public CorrelationMatrixTarget(final CorrMatrix cm) {
        this.corrMatrix = cm;
    }
}
