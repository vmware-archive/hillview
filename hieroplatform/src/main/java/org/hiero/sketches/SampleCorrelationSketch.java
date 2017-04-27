package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

public class SampleCorrelationSketch implements ISketch<ITable, CorrMatrix>{

    private final List<String> colNames;
    private final int numCols;
    private final double samplingRate;

    public SampleCorrelationSketch(List<String> colNames, double p) {
        this.colNames= colNames;
        this.numCols = colNames.size();
        this.samplingRate = p;
    }

    public SampleCorrelationSketch(List<String> colNames) {
        this.colNames= colNames;
        this.numCols = colNames.size();
        this.samplingRate = 0.1;
    }

    @Nullable
    @Override
    public CorrMatrix zero() {
        return new CorrMatrix(this.numCols);
    }

    @Nullable
    @Override
    public CorrMatrix add(@Nullable CorrMatrix left, @Nullable CorrMatrix right) {
        for (int i = 0; i < this.numCols; i++)
            for (int j = i; j < this.numCols; j++)
                left.update(i, j, right.get(i,j));
        left.count += right.count;
        return left;
    }

    @Override
    public CorrMatrix create(ITable data) {
        for (String col : this.colNames) {
            if (!data.getSchema().getColumnNames().contains(col))
                throw new InvalidParameterException("No column found with the name: " + col);
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Projection Sketch requires columm to be " +
                        "integer or double: " + col);
        }
        CorrMatrix cm = new CorrMatrix(this.numCols);
        double valj, valk;
        IRowIterator rowIt = data.getMembershipSet().sample(this.samplingRate).getIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            for (int j = 0; j < this.numCols; j++) {
                valj = data.getColumn(this.colNames.get(j)).asDouble(i, null);
                cm.update(j, j, valj * valj);
                for (int k = j + 1; k < this.numCols; k++) {
                    valk = data.getColumn(this.colNames.get(k)).asDouble(i, null);
                    cm.update(j, k, valj * valk);
                }
            }
            i = rowIt.getNextRow();
        }
        cm.count = data.getNumOfRows();
        return cm;
    }
}
