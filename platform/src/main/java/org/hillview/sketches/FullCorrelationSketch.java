package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

/**
 * This class is very similar to the SampleCorrelationSketch, apart from two important differences. First, it uses
 * the full data (it doesn't sample). Second, it rescales the columns by using the mean and standard deviation from
 * the BasicColStats's.
 */
public class FullCorrelationSketch implements ISketch<ITable, CorrMatrix> {
    private final Map<String, BasicColStats> basicColStatsMap;
    private final List<String> colNames;

    public FullCorrelationSketch(List<String> colNames, Map<String, BasicColStats> bcss) {
        this.colNames = colNames;
        this.basicColStatsMap = bcss;
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
        for (int l=0; l < this.colNames.size(); l++)
            iCols[l] = table.getColumn(this.colNames.get(l));
        CorrMatrix cm = new CorrMatrix(this.colNames);
        IRowIterator rowIt = table.getRowIterator();
        int i = rowIt.getNextRow();
        double valJ, valK;
        while (i != -1) {
            for (int j = 0; j < this.colNames.size(); j++) {
                valJ = iCols[j].asDouble(i, null);
                // Remove the mean to center the data in the column.
                valJ -= this.basicColStatsMap.get(this.colNames.get(j)).getMoment(1);
                // Divide by the standard deviation to make the column have unit variance.
                valJ /= Math.sqrt(this.basicColStatsMap.get(this.colNames.get(j)).getMoment(2));
                cm.update(j, j, valJ * valJ);
                for (int k = j + 1; k < this.colNames.size(); k++) {
                    valK = iCols[k].asDouble(i, null);
                    // Remove the mean to center the data.
                    valK -= this.basicColStatsMap.get(this.colNames.get(k)).getMoment(1);
                    // Divide by the standard deviation to make the column have unit variance.
                    valK /= Math.sqrt(this.basicColStatsMap.get(this.colNames.get(k)).getMoment(2));
                    cm.update(j, k, valJ * valK);
                }
            }
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
        for (int i = 0; i < this.colNames.size(); i++)
            for (int j = i; j < this.colNames.size(); j++)
                left.update(i, j, right.get(i,j));
        left.count += right.count;
        return left;
    }
}
