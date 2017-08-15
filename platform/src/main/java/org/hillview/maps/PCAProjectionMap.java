package org.hillview.maps;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.IMap;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.LinAlg;
import org.jblas.DoubleMatrix;

import java.io.Serializable;
import java.util.List;

/**
 * This map computes the {numProjections} principal components of the data specified by the column names.
 * Subsequently, the same data is projected along those principal components.
 * The result is the concatenation of the original table and the result.
 */
public class PCAProjectionMap implements IMap<ITable, ITable>, Serializable, IJson {
    private final int numProjections;
    private final List<String> colNames;

    public PCAProjectionMap(List<String> colNames, int numProjections) {
        this.colNames = colNames;
        this.numProjections = numProjections;
    }

    @Override
    public ITable apply(ITable table) {
        FullCorrelationSketch fcs = new FullCorrelationSketch(this.colNames);
        CorrMatrix corrMatrix = fcs.create(table);
        DoubleMatrix cm = new DoubleMatrix(corrMatrix.getCorrelationMatrix());
        DoubleMatrix eigs = LinAlg.eigenVectors(cm, this.numProjections);
        LinearProjectionMap lpm = new LinearProjectionMap(this.colNames, eigs, "PCA", null);
        return lpm.apply(table);
    }
}
