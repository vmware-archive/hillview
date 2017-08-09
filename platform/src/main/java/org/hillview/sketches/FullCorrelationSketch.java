package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * This class computes the correlations between different columns in the table.
 * This class is very similar to the SampleCorrelationSketch, apart from two important differences. First, it uses
 * the full data (it doesn't sample). Second, it rescales the columns by using the mean and standard deviation from
 * the BasicColStats's.
 */
public class FullCorrelationSketch implements ISketch<ITable, CorrMatrix> {
    private final List<String> colNames;

    public FullCorrelationSketch(List<String> colNames) {
        this.colNames = colNames;
    }

    @Override
    public CorrMatrix create(ITable table) {
        for (String col : this.colNames) {
            if ((table.getSchema().getKind(col) != ContentsKind.Double) &&
                    (table.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Correlation Sketch requires column to be " +
                        "integer or double: " + col);
        }
        IColumn[] cols = new IColumn[this.colNames.size()];
        for (int l = 0; l < this.colNames.size(); l++)
            cols[l] = table.getColumn(this.colNames.get(l));
        CorrMatrix cm = new CorrMatrix(this.colNames);

        int nRows = table.getNumOfRows();
        for (int i = 0; i < this.colNames.size(); i++) {
            double colSum = 0;
            for (int j = i; j < this.colNames.size(); j++) {
                IRowIterator rowIt = table.getRowIterator();
                int row = rowIt.getNextRow();
                double dotProduct = 0;
                while (row >= 0) {
                    double valI = cols[i].asDouble(row, null);
                    double valJ = cols[j].asDouble(row, null);
                    if (j == i)
                        colSum += valI;
                    dotProduct += valI * valJ;
                    row = rowIt.getNextRow();
                }
                cm.put(i, j, dotProduct / nRows);
            }
            cm.means[i] = colSum / nRows;
        }
        cm.count = nRows;
        return cm;
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

        CorrMatrix result = new CorrMatrix(this.colNames);
        double alpha = (double) left.count / (left.count + right.count);

        for (int i = 0; i < this.colNames.size(); i++) {
            result.means[i] = alpha * left.means[i] + (1 - alpha) * right.means[i];
            for (int j = i; j < this.colNames.size(); j++) {
                result.put(i, j, alpha * left.get(i, j) + (1 - alpha) * right.get(i, j));
            }
        }
        result.count = left.count + right.count;
        return result;
    }
}
