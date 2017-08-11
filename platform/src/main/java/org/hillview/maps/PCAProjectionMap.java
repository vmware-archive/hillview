package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.LinAlg;
import org.jblas.DoubleMatrix;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class PCAProjectionMap implements IMap<ITable, ITable> {
    private static final Logger LOG = Logger.getLogger(PCAProjectionMap.class.getName());
    private final int numProjections;
    private final List<String> colNames;

    public PCAProjectionMap(String[] colNames, int numProjections) {
        this.colNames = Arrays.asList(colNames);
        this.numProjections = numProjections;
    }

    @Override
    public ITable apply(ITable table) {
        FullCorrelationSketch fcs = new FullCorrelationSketch(this.colNames);
        LOG.info("Computing correlation matrix");
        DoubleMatrix cm = new DoubleMatrix(fcs.create(table).getCorrelationMatrix());
        LOG.info("Computing eigenvalues");
        DoubleMatrix eigs = LinAlg.eigenVectors(cm, this.numProjections);
        LinearProjectionMap lpm = new LinearProjectionMap(this.colNames, eigs, "PCA", null);
        LOG.info("Applying projection");
        return lpm.apply(table);
    }
}
