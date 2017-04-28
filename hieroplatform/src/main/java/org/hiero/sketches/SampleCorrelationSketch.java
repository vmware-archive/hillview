package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * A sketch to compute correlations between columns, using sampling.
 */
public class SampleCorrelationSketch implements ISketch<ITable, CorrMatrix>{
    /**
     * The list of columns whose correlations we wish to compute. The columns are must be of type
     * Int or Double.
     */
    private final List<String> colNames;
    /**
     * The probability of a row being included in the sample, default value is 0.1
     */
    private final double samplingRate;

    public SampleCorrelationSketch(List<String> colNames, double p) {
        this.colNames= colNames;
        this.samplingRate = p;
    }

    public SampleCorrelationSketch(List<String> colNames) {
        this.colNames= colNames;
        this.samplingRate = 0.1;
    }

    @Nullable
    @Override
    public CorrMatrix zero() {
        return new CorrMatrix(this.colNames);
    }

    @Nullable
    @Override
    public CorrMatrix add(@Nullable CorrMatrix left, @Nullable CorrMatrix right) {
        for (int i = 0; i < this.colNames.size(); i++)
            for (int j = i; j < this.colNames.size(); j++)
                left.update(i, j, right.get(i,j));
        left.count += right.count;
        return left;
    }

    /**
     * We sample from the table with probability given by the sampling rate, and compute with the
     * sampled Table.
     * @param data  Data to sketch.
     * @return A correlation matrix computed over the sampled table.
     */
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
        CorrMatrix cm = new CorrMatrix(this.colNames);
        IMembershipSet sampleData = data.getMembershipSet().sample(this.samplingRate);
        cm.count = sampleData.getSize();
        IRowIterator rowIt = sampleData.getIterator();
        int i = rowIt.getNextRow();
        double valj, valk;
        while (i != -1) {
            for (int j = 0; j < this.colNames.size(); j++) {
                valj = data.getColumn(this.colNames.get(j)).asDouble(i, null);
                cm.update(j, j, valj * valj);
                for (int k = j + 1; k < this.colNames.size(); k++) {
                    valk = data.getColumn(this.colNames.get(k)).asDouble(i, null);
                    cm.update(j, k, valj * valk);
                }
            }
            i = rowIt.getNextRow();
        }
        return cm;
    }
}
