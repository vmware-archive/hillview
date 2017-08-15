package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.Converters;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class computes the correlations between different columns in the table.
 * This class is very similar to the SampleCorrelationSketch, apart from two important differences. First, it uses
 * the full data (it doesn't sample). Second, it rescales the columns by using the mean and standard deviation from
 * the BasicColStats's.
 */
public class FullCorrelationSketch implements ISketch<ITable, CorrMatrix> {
    private static final Logger LOG = Logger.getLogger(FullCorrelationSketch.class.getName());
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
            if (table.getSchema().getDescription(col).allowMissing) {
                throw new InvalidParameterException("Correlation Sketch requires column to not allow missing data: " +
                        col);
            }
        }
        CorrMatrix corrMatrix = new CorrMatrix(this.colNames);
        int nRows = table.getNumOfRows();

        // Convert the columns to a DoubleMatrix.
        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, this.colNames.toArray(new String[]{}), null);
        DoubleMatrix covMat = mat.transpose().mmul(mat).div(nRows);
        DoubleMatrix means = mat.columnMeans();

        for (int i = 0; i < this.colNames.size(); i++) {
            for (int j = i; j < this.colNames.size(); j++) {
                corrMatrix.put(i, j,  covMat.get(i, j));
            }
            corrMatrix.means[i] = means.get(i);
        }
        corrMatrix.count = nRows;
        return corrMatrix;
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

        // Return a zero when adding two zeros.
        if (left.count == 0 && right.count == 0)
            return this.zero();

        CorrMatrix result = new CorrMatrix(this.colNames);
        double alpha = (double) left.count / (left.count + right.count);

        for (int i = 0; i < this.colNames.size(); i++) {
            result.means[i] = alpha * left.means[i] + (1 - alpha) * right.means[i];
            for (int j = i; j < this.colNames.size(); j++) {
                double val = alpha * left.get(i, j) + (1 - alpha) * right.get(i, j);
                result.put(i, j, val);
            }
        }
        result.count = left.count + right.count;
        return result;
    }
}
