package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

public class ExactIPSketch implements ISketch<ITable, CorrMatrix>{

    private List<String> colNames;
    private int numCols;

    public ExactIPSketch(List<String> colNames) {
        this.colNames= colNames;
        this.numCols = colNames.size();
    }

    @Nullable
    @Override
    public CorrMatrix zero() {
        return new CorrMatrix(this.numCols);
    }

    @Nullable
    @Override
    public CorrMatrix add(@Nullable CorrMatrix left, @Nullable CorrMatrix right) {
        for (int i = 0; i < numCols; i++)
            for (int j = 0; j < numCols; j++)
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
        IColumn iCol1, iCol2;
        double valj, valk;
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            for (int j = 0; j < this.numCols; j++) {
                valj = data.getColumn(this.colNames.get(j)).asDouble(i, null);
                for (int k = 0; k < this.numCols; k++) {
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
