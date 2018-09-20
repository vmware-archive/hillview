package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.RpcTarget;
import org.hillview.sketches.CountSketchResult;
import org.hillview.utils.HillviewLogger;

public class CountSketchTarget extends RpcTarget {
    final CountSketchResult result;

    CountSketchTarget(final CountSketchResult result, final HillviewComputation computation) {
        super(computation);
        this.result = result;
        HillviewLogger.instance.info("Count Sketch");
        this.registerObject();
    }
}
