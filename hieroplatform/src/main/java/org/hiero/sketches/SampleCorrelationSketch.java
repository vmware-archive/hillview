package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.*;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * A sketch to compute correlations between columns, using sampling.
 */
public class SampleCorrelationSketch implements ISketch<ITable, CorrMatrix> {
    /**
     * The list of columns whose correlations we wish to compute. The columns must be of type
     * Int or Double.
     */
    private final List<String> colNames;
    private final double samplingRate;

    public SampleCorrelationSketch(List<String> colNames, double samplingRate) {
        this.colNames= colNames;
        this.samplingRate = samplingRate;
    }

    /**
     * The default probability of a row being included in the sample is 0.1
     */
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
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        for (int i = 0; i < this.colNames.size(); i++)
            for (int j = i; j < this.colNames.size(); j++)
                left.update(i, j, right.get(i,j));
        left.count += right.count;
        return left;
    }

    /**
     * We sample rows from the table with probability given by the sampling rate. We then compute
     * the exact correlation for the sampled Table.
     * @param data  Data to sketch.
     * @return A correlation matrix computed over the sampled table.
     */
    @Override
    public CorrMatrix create(ITable data) {
        for (String col : this.colNames) {
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Correlation Sketch requires column to be " +
                        "integer or double: " + col);
        }
        IColumn[] iCols = new IColumn[this.colNames.size()];
        for (int l=0; l < this.colNames.size(); l++)
            iCols[l] = data.getColumn(this.colNames.get(l));
        CorrMatrix cm = new CorrMatrix(this.colNames);
        IMembershipSet sampleData = data.getMembershipSet().sample(this.samplingRate);
        cm.count = sampleData.getSize();
        IRowIterator rowIt = sampleData.getIterator();
        int i = rowIt.getNextRow();
        double valj, valk;
        while (i != -1) {
            for (int j = 0; j < this.colNames.size(); j++) {
                valj = iCols[j].asDouble(i, null);
                cm.update(j, j, valj * valj);
                for (int k = j + 1; k < this.colNames.size(); k++) {
                    valk = iCols[k].asDouble(i, null);
                    cm.update(j, k, valj * valk);
                }
            }
            i = rowIt.getNextRow();
        }
        return cm;
    }
}
