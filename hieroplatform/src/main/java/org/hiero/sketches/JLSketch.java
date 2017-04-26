package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;
import org.hiero.utils.Randomness;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

public class JLSketch implements ISketch<ITable, JLProjection>{

    private final List<String> colNames;
    private final int lowDim;

    public JLSketch(List<String> colNames, int lowDim) {
        this.colNames= colNames;
        this.lowDim = lowDim;
    }

    @Nullable
    @Override
    public JLProjection zero() {
        return new JLProjection(0, null);
    }

    @Nullable
    @Override
    public JLProjection add(@Nullable JLProjection left, @Nullable JLProjection right) {
        for(String s: left.colNames)
            for (int i = 0; i < this.lowDim; i++) {
                double val = left.get(s, i) + right.get(s, i);
                left.update(s, i, val);
            }
        left.highDim += right.highDim;
        return left;
    }

    @Override
    public JLProjection create(ITable data) {
        for(String col : this.colNames) {
            if (!data.getSchema().getColumnNames().contains(col))
                throw new InvalidParameterException("No column found with the name: " + col);
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Projection Sketch requires columm to be " +
                        "integer or double: " + col);
        }
        JLProjection jlProj = new JLProjection(this.lowDim, this.colNames);
        long seed = System.nanoTime();
        Randomness rn = new Randomness(seed);
        int i, bit;
        double val;
        String name;
        IColumn iCol;
        IRowIterator rowIt = data.getRowIterator();
        i = rowIt.getNextRow();
        while (i != -1) {
            for (int j = 0; j < this.lowDim; j++) {
                bit = ((rn.nextInt(2) == 0) ? 1 : -1);
                for (String colName: this.colNames) {
                    iCol= data.getColumn(colName);
                    val = (iCol.isMissing(i) ? 0 : (iCol.asDouble(i, null) * bit));
                    jlProj.update(colName, j, val);
                }
            }
            i = rowIt.getNextRow();
        }
        jlProj.highDim = data.getNumOfRows();
        return jlProj;
    }
}
