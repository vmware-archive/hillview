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
        int nCols = this.colNames.size();

        // Convert the columns to a DoubleMatrix.
        DoubleMatrix mat = BlasConversions.toDoubleMatrixMissing(table, this.colNames, null, Double.NaN);

        // The number of nonmissing values per column pair
        corrMatrix.nonMissing = DoubleMatrix.ones(nCols, nCols).mul(nRows);
        for (int row = 0; row < mat.rows; row++) {
            for (int i = 0; i < mat.columns; i++) {
                if (Double.isNaN(mat.get(row, i))) {
                    mat.put(row, i, 0); // Set the value to 0 so it doesn't contribute.
                    corrMatrix.nonMissing.put(i, i, corrMatrix.nonMissing.get(i, i) - 1);
                    for (int j = i; j < mat.columns; j++) {
                        if (Double.isNaN(mat.get(row, j))) {
                            corrMatrix.nonMissing.put(i, j, corrMatrix.nonMissing.get(i, j) - 1);
                            corrMatrix.nonMissing.put(j, i, corrMatrix.nonMissing.get(j, i) - 1);
                        }
                    }
                }
            }
        }

        // Since the missing values are set to 0, they don't contribute to the covariance matrix.
        DoubleMatrix covMat = mat.transpose().mmul(mat);

        // Normalize by the number of *actual* values processed. (Also for the mean!)
        covMat.divi(corrMatrix.nonMissing);
        DoubleMatrix means = mat.columnSums().divRowVector(corrMatrix.nonMissing.diag());

        for (int i = 0; i < this.colNames.size(); i++) {
            for (int j = i; j < this.colNames.size(); j++) {
                corrMatrix.put(i, j,  covMat.get(i, j));
            }
            corrMatrix.means[i] = means.get(i);
        }
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

        CorrMatrix result = new CorrMatrix(this.colNames);

        for (int i = 0; i < this.colNames.size(); i++) {
            if (left.nonMissing.get(i, i) + right.nonMissing.get(i, i) == 0)
                result.means[i] = 0;
            else {
                double meanAlpha = left.nonMissing.get(i, i) / (left.nonMissing.get(i, i) + right.nonMissing.get(i, i));
                result.means[i] = meanAlpha * left.means[i] + (1 - meanAlpha) * right.means[i];
            }
            for (int j = i; j < this.colNames.size(); j++) {
                if (left.nonMissing.get(i, j) + right.nonMissing.get(i, j) == 0) {
                    result.put(i, j, 0);
                } else {
                    double alpha = left.nonMissing.get(i, j) / (left.nonMissing.get(i, j) + right.nonMissing.get(i, j));
                    double val = alpha * left.get(i, j) + (1 - alpha) * right.get(i, j);
                    result.put(i, j, val);
                }
            }
        }
        result.nonMissing = left.nonMissing.add(right.nonMissing);

        return result;
    }
}
