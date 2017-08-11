package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.LinAlg;
import org.jblas.DoubleMatrix;

import java.util.Arrays;
import java.util.List;

public class PCAProjectionMap implements IMap<ITable, ITable> {
    private final int numProjections;
    private final List<String> colNames;

    public PCAProjectionMap(String[] colNames, int numProjections) {
        this.colNames = Arrays.asList(colNames);
        this.numProjections = numProjections;
    }

    @Override
    public ITable apply(ITable table) {
        FullCorrelationSketch fcs = new FullCorrelationSketch(this.colNames);
        DoubleMatrix cm = new DoubleMatrix(fcs.create(table).getCorrelationMatrix());
        DoubleMatrix eigs = LinAlg.eigenVectors(cm, this.numProjections);
        LinearProjectionMap lpm = new LinearProjectionMap(this.colNames, eigs, "PCA", null);
        return lpm.apply(table);
    }
}
