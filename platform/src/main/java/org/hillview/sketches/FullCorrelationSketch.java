package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
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
        IColumn[] iCols = new IColumn[this.colNames.size()];
        for (int l = 0; l < this.colNames.size(); l++)
            iCols[l] = table.getColumn(this.colNames.get(l));
        CorrMatrix cm = new CorrMatrix(this.colNames);
        IRowIterator rowIt = table.getRowIterator();
        int i = rowIt.getNextRow();
        double valJ, valK;
        while (i != -1) {
            for (int j = 0; j < this.colNames.size(); j++) {
                valJ = iCols[j].asDouble(i, null);
                cm.updateWeighted(j, j, valJ * valJ, 1);
                for (int k = j + 1; k < this.colNames.size(); k++) {
                    valK = iCols[k].asDouble(i, null);
                    cm.updateWeighted(j, k, valJ * valK, 1);
                }
                cm.updateMean(valJ, j, 1);
            }
            cm.count++;
            i = rowIt.getNextRow();
        }
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
        for (int i = 0; i < this.colNames.size(); i++) {
            for (int j = i; j < this.colNames.size(); j++)
                left.updateWeighted(i, j, right.get(i, j), right.count);
            left.updateMean(right.means[i], i, right.count);
        }
        left.count += right.count;
        return left;
    }
}
