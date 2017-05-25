package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;
import org.hiero.utils.Randomness;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * Implements the Johnson-Lindenstrauss (JL) sketch. It projects a column of doubles down to low
 * dimensional vectors, where the projection matrix consists of random +1,-1 entries. Currently,
 * this sketch can be used to compute approximate inner products. However, it is much slower than
 * sampling based methods (SampleCorrelationSketch). A potential advantage over that method is that
 * the JL sketch gives a guarantee even if the entries are not bounded, whereas the sampling based
 * methods assume boundedness for provable guarantees.
 */
public class JLSketch implements ISketch<ITable, JLProjection>{
    /**
     * The list of columns that we wish to sketch. Currently, every column is assumed to be of type
     * int or double.
     */
    private final List<String> colNames;
    /**
     * The dimension that we wish to project down to.
     */
    private final int lowDim;

    public JLSketch(List<String> colNames, int lowDim) {
        this.colNames= colNames;
        this.lowDim = lowDim;
    }

    @Nullable
    @Override
    public JLProjection zero() {
        return new JLProjection(this.colNames, this.lowDim);
    }

    @Nullable
    @Override
    public JLProjection add(@Nullable JLProjection left, @Nullable JLProjection right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        for (String s: left.colNames) {
            double a[] = left.hMap.get(s);
            double b[] = right.hMap.get(s);
            double val[] = new double[this.lowDim];
            for (int i = 0; i < this.lowDim; i++)
                val[i] = a[i] + b[i];
            left.hMap.put(s, val);
        }
        left.highDim += right.highDim;
        return left;
    }

    /**
     * The sketch of a column is the product with a random matrix of {-1,+1} entries. The same
     * matrix is applied to every column. Currently, we discard the random bits after processing the
     * relevant row of the Table.
     * @param data  Data to sketch.
     * @return A JL projection.
     */
    @Override
    public JLProjection create(ITable data) {
        for (String col : this.colNames) {
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Projection Sketch requires column to be " +
                        "integer or double: " + col);
        }
        JLProjection jlProj = new JLProjection(this.colNames, this.lowDim);
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
